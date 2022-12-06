#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include "mpi.h"

#define N 16
#define ELECTION 100
#define OK 200     
#define COORDINATOR 300

char* rank2position(int rank) // Отображение номеров процессов на координаты траспьютерной матрицы
{
    switch(rank)
    {
        case 0: return "(0, 0)"; 
        case 1: return "(0, 1)"; 
        case 2: return "(0, 2)";
        case 3: return "(0, 3)"; 
        case 4: return "(1, 3)"; 
        case 5: return "(1, 2)";
        case 6: return "(1, 1)"; 
        case 7: return "(2, 1)"; 
        case 8: return "(2, 2)";
        case 9: return "(2, 3)"; 
        case 10: return "(3, 3)"; 
        case 11: return "(3, 2)";
        case 12: return "(3, 1)"; 
        case 13: return "(3, 0)"; 
        case 14: return "(2, 0)";
        case 15: return "(1, 0)";
    }
}


int send_to_next(int rank, int size, int *array, int tag) // попытка отправить сообщение следующему процессу в траспьютерной матрице
{
    MPI_Request request;
    MPI_Status status;
    int answer, sended = 0, next = 1;
    while (!sended)
    {
        MPI_Isend(array, size, MPI_INT, (rank + next) % size, tag, MPI_COMM_WORLD, &request);
        MPI_Irecv(&answer, 1, MPI_INT, (rank + next) % size, OK, MPI_COMM_WORLD, &request);

        double start = MPI_Wtime();
        while (!sended) 
        {
            MPI_Test(&request, &sended, &status);
            if (MPI_Wtime() - start >= 1) // ждём секунду пока не придет ответ
            {
                printf("%s: Подтверждение от %s не пришло.\n", rank2position(rank), rank2position((rank + next)%size));
                fflush(stdout);
                MPI_Cancel(&request);
                MPI_Request_free(&request);
                break;
            }
        }
        next += 1;
    }
    return status.MPI_SOURCE;  // возвращаем номер процесса с удачной посылкой (чтобы запомнить номер живого процесса после нас)
}

int main(int argc, char* argv[]) 
{
    setvbuf(stdout, NULL, _IOLBF, BUFSIZ);
    setvbuf(stderr, NULL, _IOLBF, BUFSIZ);

    int size, rank, next, circle_start, answer = 1, new_coordinator, start_coordinator_circle = 0; // circle_start: номер инициатора; start_coordinator_circle: флаг процесса, который начинает рассылку КООРДИНАТОР
    int* array = (int*) malloc(size * sizeof(int)); // массив для передачи сообщений
    bool state;

    MPI_Init(&argc, &argv);               
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    srand(time(NULL) * rank);

    if (argc == 2)
        circle_start = atoi(argv[1]);
    else // неверный запуск программы
    {
        if (rank == 0)
            printf("Использовать: mpirun --oversubscribe -n 16 a.out <НОМЕР ИНИЦИАТОРА>\n");
        MPI_Finalize();
        return 0;
    }

    if (size != N || circle_start >= N) // проверка корректности введеленных данных
    {
        if (rank == 0) 
        {
            if (size != N) 
                printf("Задано неверное колличество процессов: ожидается 16\n");
            if (circle_start >= N) 
                printf("Задан неверный номер первого процесса: ожидается номер меньше 16\n");
        }
        MPI_Finalize();
        return 0;
    }

    if (rank == circle_start)
        printf("Созданные процессы транспьютерной матрицы:\n");
    
    MPI_Barrier(MPI_COMM_WORLD);
    printf("%d ", rank);
    fflush(stdout);
    
    if ((double) rand() / RAND_MAX < 0.5) // убиваем случайные процессы; состояние: работает / не работает
        state = true;
    else
        state = false;

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == circle_start)
    {
        state = true;
        printf("\nЖивые процессы матрицы перед началом кругового алгоритма:\n");
        fflush(stdout);
    }

    MPI_Barrier(MPI_COMM_WORLD);    
    if (state == true)
    {
        printf("%d ", rank);
        fflush(stdout);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (state == false) 
    {
        MPI_Finalize();
        return 0;
    }
     
    
    for (int i = 0; i < size; i += 1) 
        array[i] = 0;
    MPI_Request request;
    MPI_Status status;

    if (rank == circle_start) // отправляем сообщение от стартового процесса следующему со своим номером
    {
        printf("\n%s: Запустил круговой алгоритм\n", rank2position(rank));
        fflush(stdout);
        int sended = 0;
        array[rank] = 1;
        next = send_to_next(rank, size, array, ELECTION); // запоминаем следующий живой процесс
    }

    // Выборы
    MPI_Irecv(array, size, MPI_INT, MPI_ANY_SOURCE, ELECTION, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, &status);
    printf("%s: Получил массив от %s\n", rank2position(rank), rank2position(status.MPI_SOURCE));
    fflush(stdout);

    if (array[rank] == 1) // если массив уже был у нас - начинаем рассылку КООРДИНАТОР
    {
        start_coordinator_circle = 1;
        for (int i = size - 1; i >= 0; i -= 1) // поиск нового координатора в массиве
            if (array[i] != 0) 
                new_coordinator = i;
        printf("%s: Новый координатор: %s\n", rank2position(rank), rank2position(new_coordinator));
        fflush(stdout);
        MPI_Isend(&answer, 1, MPI_INT, status.MPI_SOURCE, OK, MPI_COMM_WORLD, &request);
        MPI_Isend(&new_coordinator, 1, MPI_INT, next, COORDINATOR, MPI_COMM_WORLD, &request);
    }
    else // иначе продолжаем круг
    {
        array[rank] = 1;
        MPI_Isend(&answer, 1, MPI_INT, status.MPI_SOURCE, OK, MPI_COMM_WORLD, &request);
        next = send_to_next(rank, size, array, ELECTION); // запоминаем следующий живой процесс
    }
    
    // Рассылка координатора
    MPI_Irecv(&new_coordinator, 1, MPI_INT, MPI_ANY_SOURCE, COORDINATOR, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, &status);
    if (!start_coordinator_circle) 
    {
        printf("%s: Новый координатор: %s\n", rank2position(rank), rank2position(new_coordinator));
        fflush(stdout);
        MPI_Isend(&new_coordinator, 1, MPI_INT, next, COORDINATOR, MPI_COMM_WORLD, &request);
    }

    MPI_Finalize();
    return 0;
}