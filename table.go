package gokey

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type ITable interface {
	Set(key, val string) error
	SetEX(key, val string, exp time.Time) error
	Delete(key string) error
	Get(key string) (map[string]string, error)
}

type Table struct {
	Name string
	Conn net.Conn
}

func (t *Table) Set(key, val string) error {
	// SET key value pair in a table
	_, err := t.Conn.Write([]byte(fmt.Sprintf("SET %s %s %s", key, val, t.Name)))
	if err != nil {
		return fmt.Errorf("error: %s", err.Error())
	}

	// the repsonse from the previous action
	buffer := make([]byte, 1024)
	len, err := t.Conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("error: %s", err.Error())
	}
	switch string(buffer[:len]) {
	case "success":
		return nil
	default:
		return fmt.Errorf(string(buffer[:len]))
	}
}

func (t *Table) SetEX(key, val string, exp int) error {
	// SET key value pair in a table
	// the pair expires in exp milliseconds
	fmt.Println(exp)
	_, err := t.Conn.Write([]byte(fmt.Sprintf("SETEX %s %s %s %v", key, val, t.Name, exp)))
	if err != nil {
		return fmt.Errorf("error: %s", err.Error())
	}

	// the repsonse from the previous action
	buffer := make([]byte, 1024)
	len, err := t.Conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("error: %s", err.Error())
	}
	switch string(buffer[:len]) {
	case "success":
		return nil
	default:
		return fmt.Errorf(string(buffer[:len]))
	}
}

func (t *Table) Delete(key string) error {
	// DELETE a pair in a table using the key
	_, err := t.Conn.Write([]byte(fmt.Sprintf("DELETE %s %s", key, t.Name)))
	if err != nil {
		return fmt.Errorf("error: %s", err.Error())
	}

	// the repsonse from the previous action
	buffer := make([]byte, 1024)
	len, err := t.Conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("error: %s", err.Error())
	}
	switch string(buffer[:len]) {
	case "success":
		return nil
	default:
		return fmt.Errorf(string(buffer[:len]))
	}
}

func (t *Table) Get(key string) (string, error) {

	// map containing "key:val" pairs that will be returned
	var data_map string

	// sending GET message
	_, err := t.Conn.Write([]byte(fmt.Sprintf("GET %s %s", key, t.Name)))
	if err != nil {
		return data_map, fmt.Errorf("error: %s", err.Error())
	}

	// response from the previous action
	buffer := make([]byte, 1024)
	len, err := t.Conn.Read(buffer)
	if err != nil {
		return data_map, fmt.Errorf("error: %s", err.Error())
	}
	switch string(buffer[:len]) {
	case "invalid database name":
		return "", fmt.Errorf("no table exists")
	default:
		data_map = string(buffer[:len])
		return data_map, nil
	}

}

func (t *Table) GetAll() (map[string]string, error) {
	// map containing "key:val" pairs that will be returned
	var data_map map[string]string

	// sending GET message
	_, err := t.Conn.Write([]byte(fmt.Sprintf("GET * %s", t.Name)))
	if err != nil {
		return data_map, fmt.Errorf("error: %s", err.Error())
	}

	// response from the previous action
	buffer := make([]byte, 1024)
	len, err := t.Conn.Read(buffer)
	if err != nil {
		return data_map, fmt.Errorf("error: %s", err.Error())
	}
	switch string(buffer[:len]) {
	case "invalid database name":
		return data_map, fmt.Errorf("no table exists")
	default:
		err = json.Unmarshal(buffer[:len], &data_map)
		if err != nil {
			return data_map, err
		}
		return data_map, nil
	}
}
