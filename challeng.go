package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const chunk_size = 500
const channel_size = 100
const cancel = "CANCELAR"
const percentil_size = 10

type register struct {
	user           string
	operation_type string
	ammount        int64
}

type accum struct {
	summ     int64
	quantity int
}

type user struct {
	name     string
	quantity int
}

type type_user struct {
	operation_type, user string
}

type type_ammount struct {
	operation_type string
	ammount        int64
}

type sliced_mean struct {
	mean     float64
	quantity float64
}

type partial_information struct {
	quantity_by_type_user map[type_user]int
	max_users_by_type     map[string]user
	means_by_type         map[string]sliced_mean
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func process_chunk(chunk []string, result chan partial_information, percentil chan type_ammount) {
	amount_by_type := make(map[string]accum)
	quantity_by_type_user := make(map[type_user]int)
	max_users_by_type := make(map[string]user)
	means_by_type := make(map[string]sliced_mean)

	for _, line := range chunk {
		// Obteniendo datos
		split := strings.Split(line, " ")
		next_user := strings.Split(split[0][1:len(split[0])-1], ":")[1]
		operation_type := strings.Split(split[1][1:len(split[1])-1], ":")[1]
		ammount, _ := strconv.Atoi(strings.Split(split[2][1:len(split[2])-1], ":")[1])

		// Acumular por tipo de operacion
		m, _ := amount_by_type[operation_type]
		m.summ += int64(ammount)
		m.quantity++
		amount_by_type[operation_type] = accum{m.summ, m.quantity}

		// Contar cantidad de operaciones por usuario
		j, _ := quantity_by_type_user[type_user{operation_type, next_user}]
		j++
		quantity_by_type_user[type_user{operation_type, next_user}] = j

		// Llevar rgistro del máximo
		max, _ := max_users_by_type[operation_type]
		if max.quantity < j {
			max_users_by_type[operation_type] = user{next_user, j}
		}

		// Datos para calcular el percentil
		if rand.Intn(1000) < 100 {
			percentil <- type_ammount{operation_type, int64(ammount)}
		}
	}

	// Procesar la info recolectada para calcular los promedios parciales
	for k, v := range amount_by_type {
		means_by_type[k] = sliced_mean{float64(v.summ) / float64(v.quantity), float64(v.quantity)}
	}

	result <- partial_information{quantity_by_type_user, max_users_by_type, means_by_type}
}

func merge(info1, info2 partial_information, chann chan partial_information) {

	// Unificar Cantidades y el máximo
	for type_user_from_2, quantity_in_2 := range info2.quantity_by_type_user {
		quantity_in_1, _ := info1.quantity_by_type_user[type_user_from_2]
		total := quantity_in_1 + quantity_in_2
		info1.quantity_by_type_user[type_user_from_2] = total

		max_in_1, _ := info1.max_users_by_type[type_user_from_2.operation_type]

		if max_in_1.quantity < total {
			info1.max_users_by_type[type_user_from_2.operation_type] = user{type_user_from_2.user, total}
		}
	}

	// Unificar promedios
	for type_2, sliced_mean_2 := range info2.means_by_type {
		mean_2 := sliced_mean_2.mean
		quantity_2 := sliced_mean_2.quantity

		sliced_mean_1, ok := info1.means_by_type[type_2]

		if ok {
			mean_1 := sliced_mean_1.mean
			quantity_1 := sliced_mean_1.quantity
			new_quantity := quantity_1 + quantity_2
			new_mean := mean_1*quantity_1/new_quantity + mean_2*quantity_2/new_quantity
			info1.means_by_type[type_2] = sliced_mean{new_mean, new_quantity}
		} else {
			info1.means_by_type[type_2] = sliced_mean{mean_2, quantity_2}
		}
	}

	chann <- partial_information{info1.quantity_by_type_user, info1.max_users_by_type, info1.means_by_type}

}

func percentil(chann chan type_ammount, percentil chan map[string][]int64, percentil_by_type map[string][]int64, mutex *sync.Mutex) {
	for {
		data := <-chann
		if data.operation_type == cancel {
			break
		}
		mutex.Lock()
		operation_type, _ := percentil_by_type[data.operation_type]
		operation_type = append(operation_type, data.ammount)
		percentil_by_type[data.operation_type] = operation_type
		mutex.Unlock()
	}
}

func main() {
	var validREG = regexp.MustCompile(`.*\[user:.*].+\[type:.*].*\[ammount:.*]`)
	var mutexMap = &sync.Mutex{}
	var mutexChan = &sync.Mutex{}
	percentil_by_type := make(map[string][]int64)

	results_chann := make(chan partial_information, channel_size)
	percentil_chann := make(chan type_ammount)
	percentil_result_chan := make(chan map[string][]int64)

	// Procesos que van recolectando los datos para calcular el percentil
	for i := 0; i < percentil_size; i++ {
		go func() {
			percentil(percentil_chann, percentil_result_chan, percentil_by_type, mutexMap)
		}()
	}

	var wg sync.WaitGroup

	//	file, error := os.Open("./proband.log")
	file, error := os.Open("./movements.log")
	check(error)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	count := 0

	// Divido el input de entrada para ir procesando por partes
	tok := scanner.Scan()
	for tok {
		var chunk []string
		for i := 0; i < chunk_size && tok; i++ {
			line := scanner.Text()
			line_ok := validREG.MatchString(line)
			if line_ok {
				chunk = append(chunk, line)
			}
			tok = scanner.Scan()
		}

		if len(chunk) > 0 {
			count++
			wg.Add(1)
			go func() {
				//				fmt.Println("CHUNK")
				process_chunk(chunk, results_chann, percentil_chann)
				wg.Done()
			}()
		}
	}

	//Varios procesos para la unificación de promedios
	for i := 0; i < count-1; i++ {
		wg.Add(1)
		go func() {
			//			fmt.Println("Merge")
			mutexChan.Lock()
			tmp1 := <-results_chann
			tmp2 := <-results_chann
			mutexChan.Unlock()
			merge(tmp1, tmp2, results_chann)
			wg.Done()
		}()
	}

	wg.Wait()

	// La última lectura tiene todo los datos unificados
	result := <-results_chann

	// Señal para liberar procesos que acumulan datos para el percentil
	for i := 0; i < percentil_size; i++ {
		percentil_chann <- type_ammount{cancel, int64(0)}
	}

	fmt.Println("Promedios")
	for k, v := range result.means_by_type {
		fmt.Printf("Tipo de Operacion: %s, Mean: %f, Quantity: %d\n", k, v.mean, int64(v.quantity))
	}

	fmt.Println("Usuarios con mas operaciones")
	for k, v := range result.max_users_by_type {
		fmt.Printf("Tipo de Operacion: %s, User: %s, Quantity: %d\n", k, v.name, v.quantity)
	}

	fmt.Println("Percentiles")
	for k, v := range percentil_by_type {
		sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
		p := float64(len(v)) * 0.95
		fmt.Printf("Tipo de Operacion: %s, Percentil: %d, From: %d\n", k, v[int(p)], len(v))
	}
}
