package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	PK_FIEALD_NAME = "__primary_key"
	IS_NULL        = "YES"
	DEFAULT_VALUE  = ""
	DEFAULT_OFFSET = 0
	DEFAULT_LIMIT  = 5
)

type Response map[string]interface{}

func __err_panic(err error) {
	if err != nil {
		panic(err)
	}
}

type Item struct {
	columnName string
	columnType *sql.ColumnType
	pointer    []string
}

type Column struct {
	pk           string
	field        sql.NullString
	fieldType    sql.NullString
	collation    sql.NullString
	isNull       sql.NullString
	isKey        sql.NullString
	defaultValue sql.NullString
	extra        sql.NullString
	privileges   sql.NullString
	comment      sql.NullString
}

type Handler struct {
	DB     *sql.DB
	tables map[string]map[string]Column
}

func (h *Handler) Router(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-type", "application/json")

	if r.URL.Path == "/" {
		h.Index(w, r)
		return
	}
	pathParts := strings.Split(r.URL.Path, "/")
	if _, ok := h.tables[pathParts[1]]; !ok {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"error": "unknown table"}`)
		return
	}

	switch r.Method {
	case "GET":
		if len(pathParts) == 2 {
			h.List(w, r)
			break
		}

		if len(pathParts) == 3 {
			h.Get(w, r)
			break
		}

		w.WriteHeader(http.StatusNotFound)
	case "POST":
		h.Update(w, r)
	case "PUT":
		h.Add(w, r)
	case "DELETE":
		h.Delete(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, `{"response": "bad request"}`)
	}

}

func (h *Handler) Index(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	tables := make([]string, 0, 10)
	for key := range h.tables {
		tables = append(tables, key)
	}
	sort.Strings(tables)
	fmt.Fprintf(w, `{"response": {"tables": ["%s"]}}`, strings.Join(tables, "\", \""))
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	table := pathParts[1]
	limit := DEFAULT_LIMIT
	offset := DEFAULT_OFFSET
	params, err := url.ParseQuery(r.URL.RawQuery)

	if _, ok := params["limit"]; ok {
		limit, err = strconv.Atoi(params["limit"][0])
		if err != nil {
			limit = DEFAULT_LIMIT
		}
	}

	if _, ok := params["offset"]; ok {
		offset, err = strconv.Atoi(params["offset"][0])
		if err != nil {
			offset = DEFAULT_OFFSET
		}
	}

	fmt.Println("sql::",
		"SELECT * FROM "+table+" LIMIT ? , ?", offset, limit,
	)

	records := make([]interface{}, 0, 10)
	rows, err := h.DB.Query("SELECT * FROM "+table+" LIMIT ? , ?", offset, limit)
	__err_panic(err)

	columns, err := rows.Columns()
	__err_panic(err)
	types, err := rows.ColumnTypes()
	__err_panic(err)

	readCols := make([]interface{}, len(columns))
	writeCols := make([]sql.NullString, len(columns))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}

	for rows.Next() {
		err = rows.Scan(readCols...)
		__err_panic(err)

		record := Response{}
		for i, columnType := range types {
			if !writeCols[i].Valid {
				record[columnType.Name()] = nil
				continue
			}

			switch columnType.DatabaseTypeName() {
			case "INT":
				record[columnType.Name()], err = strconv.Atoi(writeCols[i].String)
				__err_panic(err)
			default:
				record[columnType.Name()] = writeCols[i].String
			}
		}
		records = append(records, record)
	}
	// надо закрывать соединение, иначе будет течь
	rows.Close()

	response, _ := json.Marshal(Response{
		"response": Response{
			"records": records,
		},
	})

	w.Write(response)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	table := pathParts[1]
	id, err := strconv.Atoi(pathParts[2])
	__err_panic(err)

	fmt.Println("sql::",
		"SELECT * FROM `"+table+"` WHERE `"+h.tables[table][PK_FIEALD_NAME].field.String+"`=? LIMIT 1", id,
	)

	rows, err := h.DB.Query("SELECT * FROM `"+table+"` WHERE `"+h.tables[table][PK_FIEALD_NAME].field.String+"`=? LIMIT 1", id)
	__err_panic(err)
	// надо закрывать соединение, иначе будет течь
	defer rows.Close()

	columns, err := rows.Columns()
	__err_panic(err)
	types, err := rows.ColumnTypes()
	__err_panic(err)

	readCols := make([]interface{}, len(columns))
	writeCols := make([]sql.NullString, len(columns))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}

	record := Response{}
	if !rows.Next() {
		http.Error(w, `{"error": "record not found"}`, http.StatusNotFound)
		return
	}

	err = rows.Scan(readCols...)
	__err_panic(err)

	for i, columnType := range types {
		if !writeCols[i].Valid {
			record[columnType.Name()] = nil
			continue
		}

		switch columnType.DatabaseTypeName() {
		case "INT":
			record[columnType.Name()], err = strconv.Atoi(writeCols[i].String)
			__err_panic(err)
		default:
			record[columnType.Name()] = writeCols[i].String
		}
	}

	response, _ := json.Marshal(Response{
		"response": Response{
			"record": record,
		},
	})

	w.Write(response)
}

func (h *Handler) Add(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	table := pathParts[1]

	item := map[string]interface{}{}

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&item)
	__err_panic(err)

	for key, column := range h.tables[table] {
		if key == PK_FIEALD_NAME {
			continue
		}

		if column.isNull.String == IS_NULL {
			continue
		}

		if _, ok := item[key]; !ok {
			item[key] = DEFAULT_VALUE
		}
	}

	keys := make([]string, 0, len(item))
	values := make([]interface{}, 0, len(item))
	for key, value := range item {
		if _, ok := h.tables[table][key]; !ok {
			//ignore unknown fields
			continue
		}

		if column := h.tables[table][key]; column.isKey.String != "" {
			// ignore primary keys
			continue
		}

		keys = append(keys, key)
		values = append(values, value)
	}

	placeholders := make([]string, len(keys))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	fmt.Println("sql::",
		"INSERT INTO "+table+" (`"+strings.Join(keys, "`, `")+"`) VALUES ("+strings.Join(placeholders, ", ")+")",
		values,
	)

	// в целям упрощения примера пропущена валидация
	result, err := h.DB.Exec(
		"INSERT INTO "+table+" (`"+strings.Join(keys, "`, `")+"`) VALUES ("+strings.Join(placeholders, ", ")+")", values...
	)
	__err_panic(err)

	lastID, err := result.LastInsertId()
	__err_panic(err)

	response, _ := json.Marshal(Response{
		"response": Response{
			h.tables[table][PK_FIEALD_NAME].field.String: lastID,
		},
	})

	w.Write(response)
}

func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	table := pathParts[1]
	id, err := strconv.Atoi(pathParts[2])
	__err_panic(err)

	item := map[string]interface{}{}
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(&item)
	__err_panic(err)

	keys := make([]string, 0, len(item))
	values := make([]interface{}, 0, len(item))
	for key, value := range item {
		if _, ok := h.tables[table][key]; !ok {
			//ignore unknown fields
			continue
		}

		if column := h.tables[table][key]; column.isKey.String != "" {
			// ignore primary keys
			err = fmt.Errorf("field %s have invalid type", key)
			continue
		}

		if _, ok := value.(string); !ok && value != nil {
			// check field type
			fmt.Printf("%+v %+v", h.tables[table][key].fieldType.String, value)
			err = fmt.Errorf("field %s have invalid type", key)
			continue
		}

		if value == nil && h.tables[table][key].isNull.String != IS_NULL {
			err = fmt.Errorf("field %s have invalid type", key)
			continue
		}

		keys = append(keys, key)
		values = append(values, value)
	}

	placeholders := make([]string, len(keys))
	for i, key := range keys {
		placeholders[i] = fmt.Sprintf("`%s` = ?", key)
	}

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)

		response, _ := json.Marshal(Response{
			"error": fmt.Sprint(err),
		})

		w.Write(response)
		return
	}

	fmt.Println("sql::",
		"UPDATE `"+table+"` SET "+
			strings.Join(placeholders, ",")+
			" WHERE `"+h.tables[table][PK_FIEALD_NAME].field.String+"` = ?",
		values,
	)

	values = append(values, id)
	// в целям упрощения примера пропущена валидация
	result, err := h.DB.Exec(
		"UPDATE `"+table+"` SET "+
			strings.Join(placeholders, ",")+
			" WHERE `"+h.tables[table][PK_FIEALD_NAME].field.String+"` = ?",
		values...,
	)
	__err_panic(err)

	affected, err := result.RowsAffected()
	__err_panic(err)

	response, _ := json.Marshal(Response{
		"response": Response{
			"updated": affected,
		},
	})

	w.Write(response)
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	table := pathParts[1]
	id, err := strconv.Atoi(pathParts[2])
	__err_panic(err)

	fmt.Println("sql::",
		"DELETE FROM `"+table+"` WHERE `"+h.tables[table][PK_FIEALD_NAME].field.String+"` = ?",
		id,
	)

	result, err := h.DB.Exec(
		"DELETE FROM `"+table+"` WHERE `"+h.tables[table][PK_FIEALD_NAME].field.String+"` = ?",
		id,
	)
	__err_panic(err)

	affected, err := result.RowsAffected()
	__err_panic(err)

	response, _ := json.Marshal(Response{
		"response": Response{
			"deleted": affected,
		},
	})

	w.Write(response)
}

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

func NewDbExplorer(DB *sql.DB) (http.Handler, error) {
	handlers := &Handler{
		DB:     DB,
		tables: make(map[string]map[string]Column),
	}
	mux := http.NewServeMux()

	tables := make([]string, 0, 5)
	rows, err := DB.Query("SHOW TABLES;")
	__err_panic(err)
	for rows.Next() {
		tableName := ""

		err = rows.Scan(&tableName)
		__err_panic(err)
		tables = append(tables, tableName)
	}
	// надо закрывать соединение, иначе будет течь
	err = rows.Close()
	__err_panic(err)

	handlers.tables = make(map[string]map[string]Column, len(tables))
	for _, tableName := range tables {
		columns, err := DB.Query("SHOW FULL COLUMNS FROM " + tableName)
		__err_panic(err)

		handlers.tables[tableName] = make(map[string]Column)
		for columns.Next() {
			column := Column{}
			err = columns.Scan(&column.field, &column.fieldType, &column.collation, &column.isNull, &column.isKey, &column.defaultValue, &column.extra, &column.privileges, &column.comment)
			__err_panic(err)
			if column.isKey.String != "" {
				handlers.tables[tableName][PK_FIEALD_NAME] = column
			}
			handlers.tables[tableName][column.field.String] = column
		}
		err = columns.Close()
		__err_panic(err)
	}

	mux.HandleFunc("/", handlers.Router)

	siteHandler := accessLogMiddleware(mux)
	siteHandler = panicMiddleware(siteHandler)

	return siteHandler, nil
}

func accessLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		fmt.Printf("[%s] %s, %s %s\n",
			r.Method, r.RemoteAddr, r.URL.Path+r.URL.RawPath, time.Since(start))
	})
}

func panicMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				fmt.Println("recovered", err)
				http.Error(w, "Internal server error", 500)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
