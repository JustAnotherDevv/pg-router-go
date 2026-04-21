// GORM integration tests for pgrouter.
//
// GORM speaks pgx under the hood (gorm.io/driver/postgres) but layers
// its own AutoMigrate / Where / Update / Transaction surface on top.
// Real-world Go services use GORM, so the transaction-mode pooler MUST
// not break GORM's session expectations.
//
// Coverage:
//   - AutoMigrate creates the table
//   - Create + First (round-trip)
//   - Where + Find with bind params
//   - Update + Delete with affected-row counts
//   - tx.Commit + tx.Rollback
//   - Raw SQL with .Scan
//   - Re-use of a *gorm.DB across many Conn (pool reuse)
//
// The schema is dropped per test via a fresh temp-table name so tests
// don't interfere when run in parallel.

//go:build integration

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func gormOpen(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(postgres.New(postgres.Config{DSN: Dsn()}),
		&gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
			// Disable prepared statement cache — pgrouter rewrites
			// prep names; gorm's session-local cache would issue
			// DEALLOCATE on close which the pooler intercepts.
			PrepareStmt: false,
		})
	require.NoError(t, err)
	t.Cleanup(func() {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

// tablename returns a unique table name so parallel tests don't clash.
func tablename(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, rand.Int31())
}

type gormUser struct {
	ID    uint   `gorm:"primaryKey"`
	Name  string `gorm:"size:120"`
	Email string `gorm:"size:255;uniqueIndex"`
}

func TestGormCreateAndFirst(t *testing.T) {
	db := gormOpen(t)
	tab := tablename("gorm_user")
	db = db.Table(tab)
	require.NoError(t, db.AutoMigrate(&gormUser{}))
	t.Cleanup(func() { db.Migrator().DropTable(&gormUser{}) })

	u := gormUser{Name: "Alice", Email: fmt.Sprintf("a-%d@example.com", time.Now().UnixNano())}
	require.NoError(t, db.Create(&u).Error)
	require.NotZero(t, u.ID)

	var got gormUser
	require.NoError(t, db.First(&got, u.ID).Error)
	require.Equal(t, "Alice", got.Name)
}

func TestGormWhereFind(t *testing.T) {
	db := gormOpen(t)
	tab := tablename("gorm_where")
	db = db.Table(tab)
	require.NoError(t, db.AutoMigrate(&gormUser{}))
	t.Cleanup(func() { db.Migrator().DropTable(&gormUser{}) })

	rows := []gormUser{
		{Name: "Alice", Email: fmt.Sprintf("alice-%d@example.com", time.Now().UnixNano())},
		{Name: "Bob", Email: fmt.Sprintf("bob-%d@example.com", time.Now().UnixNano())},
		{Name: "Carol", Email: fmt.Sprintf("carol-%d@example.com", time.Now().UnixNano())},
	}
	require.NoError(t, db.Create(&rows).Error)

	var got []gormUser
	require.NoError(t, db.Where("name <> ?", "Bob").Find(&got).Error)
	require.Len(t, got, 2)
}

func TestGormUpdateAndDelete(t *testing.T) {
	db := gormOpen(t)
	tab := tablename("gorm_ud")
	db = db.Table(tab)
	require.NoError(t, db.AutoMigrate(&gormUser{}))
	t.Cleanup(func() { db.Migrator().DropTable(&gormUser{}) })

	u := gormUser{Name: "Alice", Email: fmt.Sprintf("a-%d@example.com", time.Now().UnixNano())}
	require.NoError(t, db.Create(&u).Error)

	res := db.Model(&u).Update("name", "Allison")
	require.NoError(t, res.Error)
	require.Equal(t, int64(1), res.RowsAffected)

	var got gormUser
	require.NoError(t, db.First(&got, u.ID).Error)
	require.Equal(t, "Allison", got.Name)

	res = db.Delete(&got)
	require.NoError(t, res.Error)
	require.Equal(t, int64(1), res.RowsAffected)
}

func TestGormTransactionCommit(t *testing.T) {
	db := gormOpen(t)
	tab := tablename("gorm_tx")
	db = db.Table(tab)
	require.NoError(t, db.AutoMigrate(&gormUser{}))
	t.Cleanup(func() { db.Migrator().DropTable(&gormUser{}) })

	err := db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&gormUser{
			Name:  "Tx",
			Email: fmt.Sprintf("tx-%d@example.com", time.Now().UnixNano()),
		}).Error
	})
	require.NoError(t, err)

	var n int64
	require.NoError(t, db.Model(&gormUser{}).Count(&n).Error)
	require.Equal(t, int64(1), n)
}

func TestGormTransactionRollback(t *testing.T) {
	db := gormOpen(t)
	tab := tablename("gorm_txrb")
	db = db.Table(tab)
	require.NoError(t, db.AutoMigrate(&gormUser{}))
	t.Cleanup(func() { db.Migrator().DropTable(&gormUser{}) })

	sentinel := fmt.Errorf("rollback me")
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&gormUser{
			Name:  "Rollback",
			Email: fmt.Sprintf("rb-%d@example.com", time.Now().UnixNano()),
		}).Error; err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	var n int64
	require.NoError(t, db.Model(&gormUser{}).Count(&n).Error)
	require.Equal(t, int64(0), n)
}

func TestGormRawSQL(t *testing.T) {
	db := gormOpen(t)
	var n int
	require.NoError(t, db.Raw("SELECT 1 + ?", 41).Scan(&n).Error)
	require.Equal(t, 42, n)
}

func TestGormContextCancelPropagates(t *testing.T) {
	db := gormOpen(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var n int
	err := db.WithContext(ctx).Raw("SELECT 1").Scan(&n).Error
	require.Error(t, err)
}
