package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type FIO struct {
	Name       string `json:"name"`
	Surname    string `json:"surname"`
	Patronymic string `json:"patronymic,omitempty"`
}

type EnrichedFIO struct {
	FIO
	Age          int    `json:"age,omitempty"`
	Gender       string `json:"gender,omitempty"`
	Nationality  string `json:"nationality,omitempty"`
	Error        string `json:"error,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "fio-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s\n", err)
	}

	c.SubscribeTopics([]string{"FIO"}, nil)

	http.HandleFunc("/people", getPeopleHandler)           // Для получения данных с различными фильтрами и пагинацией
	http.HandleFunc("/people/add", addPersonHandler)       // Для добавления новых людей
	http.HandleFunc("/people/delete", deletePersonHandler) // Для удаления по идентификатору
	http.HandleFunc("/people/update", updatePersonHandler) // Для изменения сущности

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			var fio FIO
			err = json.Unmarshal(msg.Value, &fio)
			if err != nil {
				enrichedFIO := EnrichedFIO{
					FIO:          fio,
					Error:        "invalid_message",
					ErrorMessage: "Invalid message format",
				}
				enrichedBytes, _ := json.Marshal(enrichedFIO)
				publishEnrichedFIO(enrichedBytes)
			} else {
				enrichedFIO, err := enrichFIO(fio)
				if err != nil {
					enrichedFIO := EnrichedFIO{
						FIO:          fio,
						Error:        "enrichment_failed",
						ErrorMessage: err.Error(),
					}
					enrichedBytes, _ := json.Marshal(enrichedFIO)
					publishEnrichedFIO(enrichedBytes)
				} else {
					enrichedBytes, _ := json.Marshal(enrichedFIO)
					publishEnrichedFIO(enrichedBytes)
				}
			}
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

}

func publishEnrichedFIO(enrichedBytes []byte) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	topic := "FIO_ENRICHED" // change to the actual topic name

	deliveryChan := make(chan kafka.Event)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          enrichedBytes,
	}, deliveryChan)
	if err != nil {
		log.Fatalf("Failed to produce message: %s\n", err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Fatalf("Delivery failed: %v\n", m.TopicPartition.Error)
	}

	close(deliveryChan)

	p.Close()
}

func enrichFIO(fio FIO) (EnrichedFIO, error) {
	enrichedFIO := EnrichedFIO{
		FIO: fio,
	}

	age, err := getAge(fio.Name)
	if err != nil {
		return enrichedFIO, fmt.Errorf("Failed to get age: %s", err)
	}
	enrichedFIO.Age = age

	gender, err := getGender(fio.Name)
	if err != nil {
		return enrichedFIO, fmt.Errorf("Failed to get gender: %s", err)
	}
	enrichedFIO.Gender = gender

	nationality, err := getNationality(fio.Name)
	if err != nil {
		return enrichedFIO, fmt.Errorf("Failed to get nationality: %s", err)
	}
	enrichedFIO.Nationality = nationality

	return enrichedFIO, nil
}

func getAge(name string) (int, error) {
	resp, err := http.Get(fmt.Sprintf("https://api.agify.io/?name=%s", name))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Age int `json:"age"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return 0, err
	}

	return result.Age, nil
}

func getGender(name string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("https://api.genderize.io/?name=%s", name))
	if err != nil {
		return "_", err
	}
	defer resp.Body.Close()

	var result struct {
		Gender string `json:"gender"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return "_", err
	}

	return result.Gender, nil
}

func getNationality(name string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("https://api.nationalize.io/?name=%s", name))
	if err != nil {
		return "_", err
	}
	defer resp.Body.Close()

	var result struct {
		Nationality string `json:"nationality"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return "_", err
	}

	return result.Nationality, nil
}

// Обработчик для получения данных с различными фильтрами и пагинацией
func getPeopleHandler(w http.ResponseWriter, r *http.Request) {
	filter := r.URL.Query().Get("filter")
	page := r.URL.Query().Get("page")
}

// Обработчик для добавления новых людей
func addPersonHandler(w http.ResponseWriter, r *http.Request) {
	var person EnrichedFIO
	err := json.NewDecoder(r.Body).Decode(&person)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// Обработчик для удаления по идентификатору
func deletePersonHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
}

// Обработчик для изменения сущности
func updatePersonHandler(w http.ResponseWriter, r *http.Request) {
	var updatedPerson EnrichedFIO
	err := json.NewDecoder(r.Body).Decode(&updatedPerson)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
