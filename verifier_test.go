package pg2mysql_test

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tompiscitell/pg2mysql"
	"github.com/tompiscitell/pg2mysql/pg2mysqlfakes"
)

var _ = Describe("Verifier", func() {
	var (
		verifier pg2mysql.Verifier
		mysql    pg2mysql.DB
		pg       pg2mysql.DB
		watcher  *pg2mysqlfakes.FakeVerifierWatcher
	)

	BeforeEach(func() {
		mysql = pg2mysql.NewMySQLDB(
			mysqlRunner.DBName,
			"root",
			"admin",
			"127.0.0.1",
			3306,
			true,
		)

		err := mysql.Open()
		Expect(err).NotTo(HaveOccurred())

		pg = pg2mysql.NewPostgreSQLDB(
			pgRunner.DBName,
			"",
			"",
			"/var/run/postgresql",
			5432,
			"disable",
		)
		err = pg.Open()
		Expect(err).NotTo(HaveOccurred())

		watcher = &pg2mysqlfakes.FakeVerifierWatcher{}
		verifier = pg2mysql.NewVerifier(pg, mysql, watcher)
	})

	AfterEach(func() {
		err := mysql.Close()
		Expect(err).NotTo(HaveOccurred())
		err = pg.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Verify", func() {
		It("notifies the watcher", func() {
			err := verifier.Verify()
			Expect(err).NotTo(HaveOccurred())
			Expect(watcher.TableVerificationDidFinishCallCount()).To(Equal(3))
			for i := 0; i < watcher.TableVerificationDidFinishCallCount(); i++ {
				_, missingRows, missingIDs := watcher.TableVerificationDidFinishArgsForCall(i)
				Expect(missingRows).To(BeZero())
				Expect(missingIDs).To(BeNil())
			}
		})

		Context("when there is data in postgres that is not in mysql", func() {
			var lastInsertID int
			BeforeEach(func() {
				err := pgRunner.DB().QueryRow("INSERT INTO table_with_id (id, name, ci_name, created_at, truthiness) VALUES (3, 'some-name', 'some-ci-name', now(), false) RETURNING id;").Scan(&lastInsertID)
				Expect(err).NotTo(HaveOccurred())
			})

			It("notifies the watcher", func() {
				err := verifier.Verify()
				Expect(err).NotTo(HaveOccurred())
				Expect(watcher.TableVerificationDidFinishCallCount()).To(Equal(3))

				expected := map[string]int64{
					"table_with_id":        1,
					"table_with_string_id": 0,
					"table_without_id":     0,
				}

				for i := 0; i < len(expected); i++ {
					tableName, missingRows, missingIDs := watcher.TableVerificationDidFinishArgsForCall(i)
					Expect(missingRows).To(Equal(expected[tableName]), fmt.Sprintf("unexpected result for %s", tableName))
					if tableName == "table_with_id" {
						Expect(missingIDs).To(Equal([]string{fmt.Sprintf("%d", lastInsertID)}))
					} else {
						Expect(missingIDs).To(BeNil())
					}
				}
			})
		})

		Context("when there is data in postgres that is in mysql", func() {
			BeforeEach(func() {
				id := 3
				name := "some-name"
				ciname := "some-ci-name"
				created_at := time.Now().UTC().Truncate(time.Second)
				truthiness := true

				stmt := "INSERT INTO table_with_id (id, name, ci_name, created_at, truthiness) VALUES ($1, $2, $3, $4, $5);"
				result, err := pgRunner.DB().Exec(stmt, id, name, ciname, created_at, truthiness)
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err := result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))

				stmt = "INSERT INTO table_with_id (id, name, ci_name, created_at, truthiness) VALUES (?, ?, ?, ?, ?);"
				result, err = mysqlRunner.DB().Exec(stmt, id, name, ciname, created_at, truthiness)
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err = result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))
			})

			It("notifies the watcher", func() {
				err := verifier.Verify()
				Expect(err).NotTo(HaveOccurred())
				Expect(watcher.TableVerificationDidFinishCallCount()).To(Equal(3))

				expected := map[string]int64{
					"table_with_id":        0,
					"table_with_string_id": 0,
					"table_without_id":     0,
				}

				for i := 0; i < len(expected); i++ {
					tableName, missingRows, missingIDs := watcher.TableVerificationDidFinishArgsForCall(i)
					Expect(missingRows).To(Equal(expected[tableName]), fmt.Sprintf("unexpected result for %s", tableName))
					Expect(missingIDs).To(BeNil())
				}
			})
		})
		Context("when a timestamp that may get rounded by mysql", func() {
			BeforeEach(func() {
				msBump, _ := time.ParseDuration("700ms")
				id := 3
				name := "some-name"
				ciname := "some-ci-name"
				created_at := time.Now().UTC().Truncate(time.Second).Add(msBump)
				truthiness := true

				stmt := "INSERT INTO table_with_id (id, name, ci_name, created_at, truthiness) VALUES ($1, $2, $3, $4, $5);"
				result, err := pgRunner.DB().Exec(stmt, id, name, ciname, created_at, truthiness)
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err := result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))

				stmt = "INSERT INTO table_with_id (id, name, ci_name, created_at, truthiness) VALUES (?, ?, ?, ?, ?);"
				result, err = mysqlRunner.DB().Exec(stmt, id, name, ciname, created_at, truthiness)
				Expect(err).NotTo(HaveOccurred())
				rowsAffected, err = result.RowsAffected()
				Expect(err).NotTo(HaveOccurred())
				Expect(rowsAffected).To(BeNumerically("==", 1))
			})

			It("notifies the watcher", func() {
				err := verifier.Verify()
				Expect(err).NotTo(HaveOccurred())
				Expect(watcher.TableVerificationDidFinishCallCount()).To(Equal(3))

				expected := map[string]int64{
					"table_with_id":        0,
					"table_with_string_id": 0,
					"table_without_id":     0,
				}

				for i := 0; i < len(expected); i++ {
					tableName, missingRows, missingIDs := watcher.TableVerificationDidFinishArgsForCall(i)
					Expect(missingRows).To(Equal(expected[tableName]), fmt.Sprintf("unexpected result for %s", tableName))
					Expect(missingIDs).To(BeNil())
				}
			})
		})
	})
})

var _ = Describe("colIDToString", func() {
	It("handles integers", func() {
		var id interface{} = 9

		Expect(pg2mysql.ColIDToString(id)).To(Equal("9"))
	})
	It("handles uuids", func() {
		someUUID := uuid.New()
		uuidBytes, _ := someUUID.MarshalBinary()

		Expect(pg2mysql.ColIDToString(uuidBytes)).To(Equal(someUUID.String()))
	})
	It("handles byte arrays that aren't uuids", func() {

		Expect(pg2mysql.ColIDToString([]byte{45, 23, 44})).To(Equal("[45 23 44]"))
	})
})
