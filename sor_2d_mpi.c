#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <mpi.h>
#include <mpi-ext.h>

#define  Max(a, b) ((a) > (b) ? (a) : (b))
#define  N 1024

double (*A)[N];
double maxeps = 0.1e-7, eps;
int itmax = 100, i, j, k, ll, shift;

MPI_Request req[4];
int myrank, ranksize;
int startrow, lastrow, nrow;
MPI_Status status[4];

void relax();
void init();
void verify(); 

int main(int an, char **as)
{
	int it;
	MPI_Init(&an, &as);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
	MPI_Comm_size(MPI_COMM_WORLD, &ranksize);
	MPI_Barrier(MPI_COMM_WORLD);
	startrow = (myrank * N) / ranksize;
	lastrow = ((myrank + 1) * N) / ranksize - 1;
	nrow = lastrow - startrow + 1;
	double time = MPI_Wtime();
	init();
	for (it = 1; it <= itmax; it++)
	{
		eps = 0.;
		relax();
		if (myrank == 0)
		{
			printf("it = %4i   eps = %f\n", it, eps);
		}
		if (eps < maxeps) break;
	}
	verify();
	if (myrank == 0)
	{
		printf("time = %f\n", MPI_Wtime() - time);
	}
    MPI_Finalize();

	return 0;
}


void init()
{ 
	A = malloc(nrow * N * sizeof(double));
	for(i = 0; i <= nrow - 1; i++)
	for(j = 0; j <= N - 1; j++)
	{
		if (i == 0 || i == N - 1 || j == 0 || j == N - 1)
		A[i][j] = 0.;
		else A[i][j] = (1. + startrow + i + j ) ;
	}
} 


void relax()
{
	if (myrank != 0)
		MPI_Irecv(&A[0][0], N, MPI_DOUBLE, myrank - 1, 1, MPI_COMM_WORLD, &req[0]);
	if (myrank != ranksize - 1)
		MPI_Isend(&A[nrow - 2][0], N, MPI_DOUBLE, myrank + 1, 1, MPI_COMM_WORLD, &req[2]);
	if (myrank != ranksize - 1)
		MPI_Irecv(&A[nrow - 1][0], N, MPI_DOUBLE, myrank + 1, 2, MPI_COMM_WORLD, &req[3]);
	if (myrank != 0)
		MPI_Isend(&A[1][0], N, MPI_DOUBLE, myrank - 1, 2, MPI_COMM_WORLD, &req[1]);
	ll = 4; 
    shift = 0;
	if (myrank == 0) {ll = 2; shift = 2;}
	if (myrank == ranksize - 1) {ll = 2;}
	if (ranksize > 1)
		MPI_Waitall(ll, &req[shift], &status[0]);

	for(i = 1; i <= nrow - 2; i++)
	{
		if (((i == 1) && (myrank == 0)) || ((i == nrow - 2) && (myrank == ranksize - 1)))
			continue;
		for (j = 1; j <= N - 2; j++)
		{ 
			double e;
			e = A[i][j];
			A[i][j] = (A[i - 1][j] + A[i + 1][j] + A[i][j - 1] + A[i][j + 1]) / 4.;
			eps = Max(eps, fabs(e - A[i][j]));
		}
	}
	if (myrank == 0)
		for (i = 1; i < ranksize; i++)
		{	
			double tmp;
			MPI_Recv(&tmp, 1, MPI_DOUBLE, MPI_ANY_SOURCE, 3, MPI_COMM_WORLD, &status[1]);
			eps = Max(eps, tmp);
		}
	if (myrank != 0)
		MPI_Ssend(&eps, 1, MPI_DOUBLE, 0, 3, MPI_COMM_WORLD);
	if (myrank != 0)
		MPI_Recv(&eps, 1, MPI_DOUBLE, 0, 4, MPI_COMM_WORLD, &status[1]);
	if (myrank == 0)
		for (i = 1; i < ranksize; i++)
			MPI_Ssend(&eps, 1, MPI_DOUBLE, i, 4, MPI_COMM_WORLD);
}


void verify()
{ 
	double s;
	s = 0.;
	for (i = 0; i <= nrow - 1; i++)
	for (j = 0; j <= N - 1; j++)
		s = s + A[i][j] * (i + 1 + startrow) * (j + 1) / (N * N);
	if (myrank == 0 && ranksize > 1)
		for (i = 1; i < ranksize; i++)
		{
			double tmp;
			MPI_Recv(&tmp, 1, MPI_DOUBLE, i, 5, MPI_COMM_WORLD, &status[1]);
			s += tmp;
		}
	if (myrank != 0)
		MPI_Ssend(&s, 1, MPI_DOUBLE, 0, 5, MPI_COMM_WORLD);
	if (myrank == 0)
	{
		printf("  S = %f\n",s);
	}
}