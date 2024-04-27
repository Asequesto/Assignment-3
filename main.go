package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

type Cursor struct {
	rds *redis.Client
	db  *sqlx.DB
}

const TeacherTable = "t_teacher"
const LessonTable = "t_lesson"

type Lesson struct {
	Id        int
	Name      string `json:"name" db:"name"`
	TeacherId int    `json:"teacher_id" db:"teacher_id"`
}

type Teacher struct {
	Id      int
	Name    string `json:"name" db:"name"`
	Surname string `json:"surname" db:"surname"`
	Degree  string `json:"degree" db:"degree"`
}

func CallError(c *gin.Context, code int, message string) {
	c.AbortWithStatusJSON(code, map[string]string{"message:": message})
}

func (cr *Cursor) cache(c *gin.Context, id int, teacher Teacher) {
	marshalledTeacher, err := json.Marshal(teacher)
	if err != nil {
		return
	}
	cr.rds.Set(c, strconv.Itoa(id), marshalledTeacher, time.Hour*2)
}

func (cr *Cursor) tryDB(c *gin.Context, id int) (Teacher, error) {
	var result Teacher
	query := fmt.Sprintf(`select l.name, t.name, t.surname, t.degree from %s l
				join %s t on t.id = l.teacher_id
				where b.id = $1`, LessonTable, TeacherTable)
	err := cr.db.Get(&result, query, id)
	return result, err
}

func (cr *Cursor) getTeacherOfLesson(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))

	if err != nil {
		CallError(c, http.StatusInternalServerError, err.Error())
		return
	}

	if id <= 0 {
		CallError(c, http.StatusBadRequest, "id cannot be negative")
		return
	}

	var teacher Teacher
	JSONbook, err := cr.rds.Get(c, strconv.Itoa(id)).Result()

	if err != nil {
		teacher, err = cr.tryDB(c, id)
		if err != nil {
			CallError(c, http.StatusInternalServerError, err.Error())
			return
		}
		cr.cache(c, id, teacher)
		c.JSON(http.StatusOK, teacher)
		return
	}

	if err := json.Unmarshal([]byte(JSONbook), &teacher); err != nil {
		teacher, err := cr.tryDB(c, id)
		if err != nil {
			CallError(c, http.StatusInternalServerError, err.Error())
			return
		}
		cr.cache(c, id, teacher)
		c.JSON(http.StatusOK, teacher)
		return
	}

	c.JSON(http.StatusOK, teacher)
}

func (cr *Cursor) createLesson(c *gin.Context) {
	var input Lesson
	if err := c.BindJSON(&input); err != nil {
		CallError(c, http.StatusBadRequest, err.Error())
		return
	}

	_, err := cr.db.Exec("insert into t_lessons(name, teacher_id) values($1, $2)")

	if err != nil {
		CallError(c, http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, nil)
}

func main() {
	db, err := sqlx.Open("postgres", "host=localhost port=%=8080 user=postgres dbname=postgres password=Asar2004 sslmode=disable")
	if err != nil {
		panic("DB connection error: " + err.Error())
	}
	redis := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	cursor := &Cursor{
		rds: redis,
		db:  db,
	}

	router := gin.New()

	router.GET("get-teacher-of-lesson", cursor.getTeacherOfLesson)
	router.PUT("add-lesson", cursor.createLesson)

	srv := http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	err = srv.ListenAndServe()
	if err != nil {
		panic("Server run error: " + err.Error())
	}

}
