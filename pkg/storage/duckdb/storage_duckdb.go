package storage

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/admpub/log"
	"github.com/admpub/log-analyzer/pkg/extraction"
	"github.com/admpub/log-analyzer/pkg/storage"
	"github.com/jmoiron/sqlx"
	"github.com/marcboeker/go-duckdb"
)

const tableName = `LogAnalyzer`

func init() {
	storage.Register(`duckdb`, newDuckDB)
}

func newDuckDB() (storage.Storager, error) {
	db, err := sqlx.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	//vt := `UNION(num BIGINT, str VARCHAR, bool BOOLEAN, float DOUBLE)`
	vt := `VARCHAR`
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS ` + tableName + ` (
Pattern    VARCHAR,
LineNumber UBIGINT,
Line       VARCHAR,
Params     MAP(VARCHAR, ` + vt + `)
);`)
	if err != nil {
		return nil, err
	}
	return &storageDuckDB{
		db: db,
	}, nil
}

type storageDuckDB struct {
	db *sqlx.DB
}

func (e *storageDuckDB) Append(extra extraction.Extraction) error {
	_, err := e.db.Exec(`INSERT INTO `+tableName+` VALUES(?, ?, ?, MAP`+AsDuckMap(extra.Params)+`)`, extra.Pattern, extra.LineNumber, extra.Line)
	return err
}

func (e *storageDuckDB) Update(extra extraction.Extraction) error {
	r, err := e.db.Query(`SELECT rowid FROM ` + tableName + ` ORDER BY rowid DESC LIMIT 1`)
	if err != nil {
		return err
	}
	defer r.Close()
	var rowid sql.NullInt64
	for r.Next() {
		err = r.Scan(&rowid)
		if err != nil {
			return err
		}
	}

	if rowid.Valid {
		var res sql.Result
		res, err = e.db.Exec(`UPDATE `+tableName+` SET Pattern=?, LineNumber=?, Line=?, Params=(MAP`+AsDuckMap(extra.Params)+`) WHERE rowid=?`, extra.Pattern, extra.LineNumber, extra.Line, rowid.Int64)
		if err == nil {
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if n == 0 {
				return fmt.Errorf(`failed to update: RowsAffected=0(rowid=%d)`, rowid.Int64)
			}
		}
	} else {
		err = e.Append(extra)
	}
	return err
}

func (e *storageDuckDB) List() ([]extraction.Extraction, error) {
	var list []extraction.Extraction
	r, err := e.db.Query(`SELECT * FROM ` + tableName + ` LIMIT 10`)
	if err != nil {
		return list, err
	}
	defer r.Close()
	for r.Next() {
		var row extraction.Extraction
		var params duckdb.Map
		err = r.Scan(&row.Pattern, &row.LineNumber, &row.Line, &params)
		if err != nil {
			return list, err
		}
		row.Params = FromDuckMap(params)
		list = append(list, row)
	}
	return list, err
}

func (e *storageDuckDB) GetLastLines(n int) (unuseds []string) {
	r, err := e.db.Query(`SELECT Line FROM ` + tableName + ` ORDER BY rowid DESC LIMIT ` + strconv.Itoa(n))
	if err != nil {
		log.Error(err)
		return nil
	}
	defer r.Close()
	lines := make([]string, 0, n)
	for r.Next() {
		var row sql.NullString
		err = r.Scan(&row)
		if err != nil {
			log.Error(err)
			return nil
		}
		lines = append(lines, row.String)
	}
	for j := len(lines) - 1; j >= 0; j-- {
		unuseds = append(unuseds, lines[j])
	}
	return
}

func (e *storageDuckDB) Close() {
	e.db.Close()
}
