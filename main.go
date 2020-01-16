package main

import (
	"fmt"

	"github.com/inflowml/logger"
	"github.com/inflowml/structql"
)

// Person defines the table schema for the people table.
type Person struct {
	ID    int32  `sql:"id" typ:"SERIAL" opt:"PRIMARY KEY"` // Each StructQL table must contain an int32 id column
	Name  string `sql:"name"`
	Age   int32  `sql:"age"`
	Email string `sql:"email"`
}

func main() {
	logger.Info("Attempting to Connect to SQL Server")

	// Basic configuration for test database connection
	dbConfig := structql.ConnectionConfig{
		Database: "testdb",          // Database Name
		User:     "structqldemo",    // User Name
		Password: "structqlpw",      // User Password
		Host:     "localhost",       // Host of DB
		Port:     "5432",            // Database Port
		Driver:   structql.Postgres, // The SQL driver required
	}

	// Connect to database and store connection receiver
	conn, err := structql.Connect(dbConfig)
	defer conn.Close()
	if err != nil {
		logger.Fatal("failed to connect to SQL database with provided ConnectionConfig: %v", err)
	}

	err = conn.CreateTableFromObject("people", Person{})
	if err != nil {
		logger.Error("failed to create people table: %v", err)
	}

	//Uncoment to populate people table
	/*err = populatePeople(conn)
	if err != nil {
		logger.Error("encountered an error while populating database: %v", err)
	}*/

	err = PrintPeople(conn)
	if err != nil {
		logger.Error("encountered an error while attempting to print table", err)
	}

	err = PrintBoomers(conn)
	if err != nil {
		logger.Error("encountered an error while attempting to print boomers", err)
	}

}

// NewPerson accepts name, age, email, and a reference to the StructQL database
// and inserts the person into a new row in the people table
func NewPerson(name string, age int32, email string, conn *structql.Connection) (int, error) {
	person := Person{
		Name:  name,
		Age:   age,
		Email: email,
	}

	id, err := conn.InsertObject("people", person)
	if err != nil {
		return id, fmt.Errorf("failed to insert person into people table: %v", err)
	}

	return id, nil
}

// PrintPeople prints all the rows in the database.
// Everyone printed through this function has their respective
// Data wrapped in a 'PEOPLE[]' block
func PrintPeople(conn *structql.Connection) error {

	// Query entire people table
	people, err := conn.SelectFrom(Person{}, "people")
	if err != nil {
		return fmt.Errorf("failed to select from people table: %v", err)
	}
	// Iterate through each row and print the data
	for _, row := range people {
		person := row.(Person) // Select returns []interface{} must be cast to person
		logger.Info("PERSID: %v, Name: %v, Age: %v, Email: %v", person.ID, person.Name, person.Age, person.Email)
	}

	return nil
}

// PrintBoomers prints all the rows in the database where the person is older than 75.
// Everyone printed through this function has their respective
// Data wrapped in a 'BOOMER[]' block
func PrintBoomers(conn *structql.Connection) error {
	// Query entire people table
	people, err := conn.SelectFromWhere(Person{}, "people", "age >= 75")
	if err != nil {
		return fmt.Errorf("failed to select from people table: %v", err)
	}
	// Iterate through each row and print the data
	for _, row := range people {
		person := row.(Person) // Select returns []interface{} must be cast to person
		logger.Info("BOOMER[ID: %v, Name: %v, Age: %v, Email: %v]", person.ID, person.Name, person.Age, person.Email)
	}

	return nil
}

func populatePeople(conn *structql.Connection) error {
	id, err := NewPerson("Jacoby Joukema", 23, "jacoby.joukema@inflowml.com", conn)
	if err != nil {
		return fmt.Errorf("failed to insert Jacoby Joukema into people table: %v", err)
	}
	logger.Info("Added Jacoby Joukema to database, assigned id: %v", id)

	id, err = NewPerson("Foo Bar", 28, "foobar@inflowml.com", conn)
	if err != nil {
		return fmt.Errorf("failed to insert Jacoby Joukema into people table: %v", err)
	}
	logger.Info("Added Foo Bar to database, assigned id: %v", id)

	id, err = NewPerson("John Henry Eden", 97, "john.henry@enclave.com", conn)
	if err != nil {
		return fmt.Errorf("failed to insert Jacoby Joukema into people table: %v", err)
	}
	logger.Info("Added John Henry Eden to database, assigned id: %v", id)

	return nil
}
