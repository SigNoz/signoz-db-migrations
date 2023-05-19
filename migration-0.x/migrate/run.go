package migrate

import (
	"github.com/jmoiron/sqlx"
)

func Run(db *sqlx.DB) error {
	// migrate dashboards
	migrateDashboards(db)

	// migrate rules
	migrateRules(db)

	return nil
}
