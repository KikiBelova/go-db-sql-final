package main

import (
	"database/sql"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (id int, err error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	const query = `
		INSERT INTO main.parcel(client, status, address, created_at)
		VALUES (:client, :status, :address, :created_at)
		RETURNING number
`
	row := s.db.QueryRow(query,
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt))

	err = row.Scan(&id)
	if err != nil {
		return 0, err
	}

	// верните идентификатор последней добавленной записи
	return
}

func (s ParcelStore) Get(number int) (p Parcel, err error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	const query = `
		SELECT number, client, status, address, created_at
		FROM main.parcel
		WHERE number = :number
`

	row := s.db.QueryRow(query, sql.Named("number", number))
	err = row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	return
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк

	const query = `
		SELECT number, client, status, address, created_at
		FROM main.parcel
		WHERE client = :client
`

	rows, err := s.db.Query(query, sql.Named("client", client))
	if err != nil {
		return nil, err
	}

	// заполните срез Parcel данными из таблицы
	var res []Parcel
	for rows.Next() {
		var p Parcel
		err = rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			return nil, err
		}

		res = append(res, p)
	}

	return res, rows.Err()
}

func (s ParcelStore) SetStatus(number int, status string) (err error) {
	// реализуйте обновление статуса в таблице parcel
	const query = `
		UPDATE main.parcel
		SET status = :status
		WHERE number = :number
`

	_, err = s.db.Exec(query, sql.Named("number", number), sql.Named("status", status))
	return
}

func (s ParcelStore) SetAddress(number int, address string) (err error) {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	// реализуйте обновление статуса в таблице parcel
	const query = `
		UPDATE main.parcel
		SET address = :address
		WHERE number = :number and status = :status
`

	_, err = s.db.Exec(query, sql.Named("number", number), sql.Named("address", address), sql.Named("status", ParcelStatusRegistered))
	return
}

func (s ParcelStore) Delete(number int) (err error) {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	const query = `
		DELETE FROM main.parcel
		WHERE number = :number and status = :status
`

	_, err = s.db.Exec(query, sql.Named("number", number), sql.Named("status", ParcelStatusRegistered))
	return
}
