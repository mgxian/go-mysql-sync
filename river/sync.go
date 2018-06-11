package river

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"gopkg.in/birkirb/loggers.v1/log"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

const (
	fieldTypeList = "list"
	// for the mysql int type to es date type
	// set the [rule.field] created_time = ",date"
	fieldTypeDate = "date"
)

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r *River
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		string(e.NextLogName),
		uint32(e.Position),
	}

	h.r.syncCh <- posSaver{pos, true}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema, table string) error {
	err := h.r.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
		return errors.Trace(err)
	}
	return nil
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.r.syncCh <- posSaver{nextPos, true}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.r.syncCh <- posSaver{nextPos, false}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}

	var sqls []string
	var err error
	switch e.Action {
	case canal.InsertAction:
		sqls, err = h.r.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		sqls, err = h.r.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		sqls, err = h.r.makeUpdateRequest(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		return errors.Errorf("make %s ES request err %v, close sync", e.Action, err)
	}

	h.r.syncCh <- sqls

	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	return nil
}

func (h *eventHandler) String() string {
	return "ESRiverEventHandler"
}

func (r *River) syncLoop() {
	bulkSize := r.c.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := r.c.FlushBulkTime.Duration
	if interval == 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	lastSavedTime := time.Now()
	sqls := make([]string, 0, 1024)

	var pos mysql.Position

	for {
		needFlush := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case []string:
				sqls = append(sqls, v...)
				needFlush = len(sqls) >= bulkSize
			}
		case <-ticker.C:
			needFlush = true
		case <-r.ctx.Done():
			return
		}

		if needFlush {
			// TODO: retry some times?
			if err := r.do(sqls); err != nil {
				log.Errorf("do SQL bulk err %v, close sync", err)
				r.cancel()
				return
			}
			sqls = sqls[0:0]
		}

		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				r.cancel()
				return
			}
		}
	}
}

// for insert and delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]string, error) {
	sqls := make([]string, 0, len(rows))

	for _, values := range rows {
		switch action {
		case canal.InsertAction:
			sql := r.makeInsertSQL(rule, values)
			sqls = append(sqls, sql)
			r.st.InsertNum.Add(1)
		case canal.DeleteAction:
			sql := r.makeDeleteSQL(rule, values)
			sqls = append(sqls, sql)
			r.st.DeleteNum.Add(1)
		default:
			log.Errorf("unknow action ----> %s", action)
		}
	}

	return sqls, nil
}

func (r *River) getNumberValue(value interface{}) (v string, err error) {
	err = nil
	switch value := value.(type) {
	case int8:
		v = fmt.Sprintf("%d", value)
		return
	case int16:
		v = fmt.Sprintf("%d", value)
		return
	case int32:
		v = fmt.Sprintf("%d", value)
		return
	case int64:
		v = fmt.Sprintf("%d", value)
		return
	case float32:
		v = fmt.Sprintf("%.6f", value)
		return
	case float64:
		v = fmt.Sprintf("%.6f", value)
		return
	default:
		err = errors.New("no support type")
		log.Info("no support type ", reflect.TypeOf(value))
		return
	}
}

func (r *River) getSetValue(column schema.TableColumn, value interface{}) (v string, err error) {
	err = nil
	switch value := value.(type) {
	case int64:
		// for binlog, SET may be int64, but for dump, SET is string
		bitmask := value
		sets := make([]string, 0, len(column.SetValues))
		for i, s := range column.SetValues {
			if bitmask&int64(1<<uint(i)) > 0 {
				sets = append(sets, s)
			}
		}
		v = "'" + strings.Join(sets, ",") + "'"
	default:
		v = "'" + value.(string) + "'"
	}
	return
}

func (r *River) getEnumValue(column schema.TableColumn, value interface{}) (v string, err error) {
	err = nil
	switch value := value.(type) {
	case int64:
		// for binlog, ENUM may be int64, but for dump, enum is string
		eNum := value - 1
		if eNum < 0 || eNum >= int64(len(column.EnumValues)) {
			// we insert invalid enum value before, so return empty
			log.Warnf("invalid binlog enum index %d, for enum %v", eNum, column.EnumValues)
			v = ""
		}

		v = "'" + column.EnumValues[eNum] + "'"
	default:
		v = "'" + value.(string) + "'"
	}
	return
}

func (r *River) getBitValue(value interface{}) (v string, err error) {
	err = nil
	switch value := value.(type) {
	case string:
		// for binlog, BIT is int64, but for dump, BIT is string
		// for dump 0x01 is for 1, \0 is for 0
		t := int(0)
		if value == "\x01" {
			t = int(1)
		}
		v = strconv.Itoa(t)
	default:
		v = strconv.Itoa(value.(int))
	}
	return
}

func (r *River) getTextValue(value interface{}) (v string, err error) {
	switch value := value.(type) {
	case []uint8:
		v = "'" + string(value) + "'"
	default:
		v = "'" + value.(string) + "'"
	}
	return
}

func (r *River) getColumnValue(column schema.TableColumn, value interface{}) (v string, err error) {
	err = nil
	if value == nil {
		v = "NULL"
		return
	}
	// log.Infof("field ---->  %s %s %d %v", column.Name, column.RawType, column.Type, value)
	switch column.Type {
	case schema.TYPE_NUMBER, schema.TYPE_FLOAT:
		return r.getNumberValue(value)
	case schema.TYPE_SET:
		return r.getSetValue(column, value)
	case schema.TYPE_BIT:
		return r.getBitValue(value)
	case schema.TYPE_ENUM:
		return r.getEnumValue(column, value)
	default:
		// the text field is MYSQL_TYPE_BLOB in the MySQL binlog, so go-mysql will use the bytes for it
		switch column.RawType {
		case "text":
			return r.getTextValue(value)
		default:
			v = "'" + value.(string) + "'"
		}
	}
	return
}

func (r *River) getWhereCondition(table *schema.Table, values []interface{}) (string, error) {
	sqlWhereField := make([]string, 0)
	sqlWhereValues := make([]string, 0)
	PKColumns := table.PKColumns
	if len(PKColumns) > 0 {
		for _, columnIdx := range PKColumns {
			column := table.GetPKColumn(columnIdx)
			name := column.Name
			sqlWhereField = append(sqlWhereField, "`"+name+"`")
			value, err := canal.GetColumnValue(table, name, values)
			v, err := r.getColumnValue(*column, value)
			if err != nil {
				log.Error(err)
			} else {
				sqlWhereValues = append(sqlWhereValues, v)
			}
		}
	} else {
		for idx, column := range table.Columns {
			name := column.Name
			sqlWhereField = append(sqlWhereField, "`"+name+"`")
			value := values[idx]

			v, err := r.getColumnValue(column, value)
			if err != nil {
				log.Error(err)
			} else {
				sqlWhereValues = append(sqlWhereValues, v)
			}
		}
	}

	whereCondition := make([]string, 0)
	for idx, field := range sqlWhereField {
		whereCondition = append(whereCondition, fmt.Sprintf("%s = %s", field, sqlWhereValues[idx]))
	}
	return strings.Join(whereCondition, " AND "), nil
}

func (r *River) makeDeleteSQL(rule *Rule, values []interface{}) string {
	// get where condition
	where, _ := r.getWhereCondition(rule.TableInfo, values)

	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;",
		r.getDestinationSchema(rule.TableInfo.Schema), rule.TableInfo.Name, where)
	return sql
}

func (r *River) makeInsertSQL(rule *Rule, values []interface{}) string {
	sqlField := make([]string, 0)
	sqlValues := make([]string, 0)
	// log.Infof("table ----> %s", rule.Table)
	for idx, column := range rule.TableInfo.Columns {
		name := column.Name
		sqlField = append(sqlField, "`"+name+"`")
		value := values[idx]

		v, err := r.getColumnValue(column, value)
		if err != nil {
			log.Error(err)
		} else {
			sqlValues = append(sqlValues, v)
		}

	}

	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUE(%s);",
		r.getDestinationSchema(rule.TableInfo.Schema), rule.TableInfo.Name,
		strings.Join(sqlField, ", "),
		strings.Join(sqlValues, ", "))
	return sql
}

func (r *River) getDestinationSchema(schema string) string {
	if ds, ok := r.schemaMap[schema]; ok {
		return ds
	}

	return schema
}

func (r *River) makeUpdateSQL(rule *Rule, beforValues []interface{}, afterValues []interface{}) string {
	sqlField := make([]string, 0)
	sqlValues := make([]string, 0)

	// log.Infof("table ----> %s", rule.Table)

	// get set field
	for idx, column := range rule.TableInfo.Columns {
		name := column.Name
		beforValue := beforValues[idx]
		afterValue := afterValues[idx]
		if reflect.DeepEqual(afterValue, beforValue) {
			continue
		}

		sqlField = append(sqlField, "`"+name+"`")
		value := afterValues[idx]

		v, err := r.getColumnValue(column, value)
		if err != nil {
			log.Error(err)
		} else {
			sqlValues = append(sqlValues, v)
		}
	}

	// get where condition
	where, _ := r.getWhereCondition(rule.TableInfo, beforValues)

	setPart := make([]string, 0)
	for idx, field := range sqlField {
		setPart = append(setPart, fmt.Sprintf("%s = %s", field, sqlValues[idx]))
	}

	sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s;",
		r.getDestinationSchema(rule.TableInfo.Schema), rule.TableInfo.Name,
		strings.Join(setPart, ", "), where)
	return sql
}

func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]string, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]string, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]string, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}
	sqls := make([]string, 0)
	for i := 0; i < len(rows); i += 2 {
		beforeRow := rows[i]
		afterRow := rows[i+1]
		sql := r.makeUpdateSQL(rule, beforeRow, afterRow)
		if sql != "" {
			sqls = append(sqls, sql)
			r.st.UpdateNum.Add(1)
		}
	}
	if len(sqls) == 0 {
		return nil, errors.Errorf("empty sqls")
	}
	return sqls, nil
}

func (r *River) do(sqls []string) error {
	if len(sqls) == 0 {
		return nil
	}

	for _, sql := range sqls {
		if err := r.db.Exec(sql); err != nil {
			log.Errorf("exec error ----> %s %s", sql, err)
		} else {
			log.Infof("exec success ----> %s", sql)
		}
	}
	return nil
}
