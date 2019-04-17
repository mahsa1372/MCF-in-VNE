#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <glpk.h>

int main(void) {

	glp_prob *lp; //define glp problem
	int a[1+1000], b[1+1000];
	double ar[1+1000]; //define coefficients of rows
	float z; //Min result
	int m = 2; //Number of virtual nodes
	int n = 3; //Number of substrate nodes
	int mm = m*(m-1); //Number of virtual links
	int nn = n*(n-1); //Number of substrate links
        int c[1+1000] = {1,1,1000};
//{2,1,2,3}; //Costs of nodes
	int d[1+1000] = {0,5,5};
//{0,5,5,5,5,5,5}; //Demands of virutal links
	int kk[1+1000] = {1,1,1,1,1,1};
//{1,1,1,1,1,1,1,1,1,1,1,1}; //Costs of substrate links: Is there any link between two nodes?
	int B[1+1000] = {0,10,0,10,10,10};
//{0,10,10,0,0,10,10,0,10,10,10,10}; //Bandwidth of substrate links
	int x[n][m]; //Whether the virtual node is mapped to substrate node
	float f[n][n][m][m];//Whether the virtual link is mapped to substrate link

	lp = glp_create_prob(); //create problem
	glp_set_prob_name(lp, "test optimierung Graph"); //define problem name
	glp_set_obj_dir(lp, GLP_MIN); //define direction of solve of the problem to minimum

	int numOfRows = (m + n) + (mm * n) + nn; //Number of Rows
	int numOfCols = (m * n) + (mm * nn); //Number of Columns

/*....................................Define Rows for Nodes...................................................*/

	glp_add_rows(lp, numOfRows);
	int h = 1;
	for (int i = 1; i < m+1; i = i+1 ) {
		glp_set_row_name(lp, h, "rows");
		glp_set_row_bnds(lp, h, GLP_FX, 1, 1);
		h++;
	} //x[1][a]+x[2][a]+x[3][a]=1 : Exact one substrate node mapped to a virtual node

	for (int i = m+1; i < m+n+1; i = i+1 ) {
		glp_set_row_name(lp, h, "rows");
		glp_set_row_bnds(lp, h, GLP_DB, 0, 1);
		h++;
	} //x[1][a]+x[1][b]<=1 : One substrate node can map maximal to one virtual node

/*...................................Define Rows for Links.................................................*/

	for (int i = 1; i < mm+1; i++) {
		for (int j = 1; j < n+1; j++) {
			glp_set_row_name(lp, h, "rows");
			glp_set_row_bnds(lp, h, GLP_FX, 0, 0);
			h++;
		}
	} //Relation between source and sinks

	for (int i = 1; i < nn+1; i++) {
		glp_set_row_name(lp, h, "rows");
		glp_set_row_bnds(lp, h, GLP_UP, 0, B[i-1]);
		h++;
	} //One substrate link can map to one virtual node

/*...................................Define Columns for Nodes.................................................*/

	glp_add_cols(lp, numOfCols);
	int hh = 1;
	for (int i = 1; i < m+1; i = i+1 ) {
		for (int j = 0; j < n; j = j+1 ) {
			glp_set_col_name(lp, hh, "columns");
		        glp_set_col_kind(lp, hh, GLP_BV);
			glp_set_col_bnds(lp, hh, GLP_DB, 0, 1);
			glp_set_obj_coef(lp, hh, c[j]);
			hh = hh + 1;
		}
	} //Define X[1][a] only 0 or 1

/*...................................Define Columns for Links........................................*/

	for (int i = 1; i < mm+1; i++) {
		for (int j = 0; j < nn; j++) {
			glp_set_col_name(lp, hh, "columns");
		        glp_set_col_kind(lp, hh, GLP_CV);
			glp_set_col_bnds(lp, hh, GLP_LO, 0, 0);
			glp_set_obj_coef(lp, hh, kk[j]);
			hh = hh + 1;
		}
	} //Define 0 <= f[12][ab]

/*...................................Define Co-Costraints for Node-Rows............................................*/

	long k = 1;
	for (int i = 1; i < m+1; i = i+1 ) {
		for (int j = 1; j < n+1; j = j+1 ) {
			a[k] = i,b[k] = k,ar[k] = 1;
//			printf("a[%ld]=%d, b[%ld]=%d;\n", k, a[k], k, b[k]);
			k = k+1;
		}
	} //For first rows: x[1][a]+x[2][a]+x[3][a]=1

//	printf("k=%ld;\n", k);
	for (int i = 1; i < n+1; i = i+1 ) {
		for (int j = 1; j < m+1; j = j+1 ) {
			a[k] = i+m, b[k] = ((n*(j-1))+i), ar[k] = 1;
//                      printf("a[%ld]=%d, b[%ld]=%d;\n", k, a[k], k, b[k]);
			k = k+1;
		}
	} //For second rows: x[1][a]+x[1][b]<=1
//      printf("k=%ld;\n", k);

/*...................................Define Co-Constraints for Links-Rows..........................................*/

	int number = (m+n)+1;
	int numberOfVLinks = 0;
	while (number < (m+n)+(n*mm)+1) {
		for (int i = 1; i < m+1; i++) {
		        for (int j = 1; j < m+1; j++) {
				if ( i == j) {

				}
				else {
					numberOfVLinks++;
					int jjjj = 0;
				        for (int ii = 1; ii < n+1; ii++) {
						int jjj = 1;
					        for (int jj = 1; jj < n+1; jj++) {
							if ( ii == jj ) {
								jjjj++;
							}
							else {
								a[k] = number,b[k] = (m*n)+(nn*(numberOfVLinks-1))+(((ii-1)*(n-1))+jjj),ar[k] = 1;
//								printf("a[%ld]=%d, b[%ld]=%d; \n", k, a[k], k, b[k]);
                                				k++;
								int iii = jj;
                                				a[k] = number,b[k] = (m*n)+(nn*(numberOfVLinks-1))+(((iii-1)*(n-1))+jjjj),ar[k] = -1;
//								printf("a[%ld]=%d, b[%ld]=%d; \n", k, a[k], k, b[k]);
                                				k++;
								jjj++;
							}
						}
						a[k] = number,b[k] = (n*(i-1))+ii,ar[k] = -1*d[numberOfVLinks];
//                                              printf("a[%ld]=%d, b[%ld]=%d, ar[%ld]=%f, %d; \n", k, a[k], k, b[k], k, ar[k], numberOfVLinks);
						k++;
						a[k] = number,b[k] = (n*(j-1))+ii,ar[k] = d[numberOfVLinks];
//                                              printf("a[%ld]=%d, b[%ld]=%d, ar[%ld]=%f, %d; \n", k, a[k], k, b[k], k, ar[k], numberOfVLinks);
						k++;
						number++;
					}
				}
			}
		}
	} //For third rows: f[][]-f[][]-x[][]+x[][]=0
//      printf("k=%ld;\n", k);

	int numbernn = (m+n)+(n*mm)+1;
	for (int i = 1; i < nn+1; i++) {
		for (int j = 1; j < mm+1; j++) {
			a[k] = numbernn, b[k] = (m*n)+(nn*(j-1)+i), ar[k] = 1;
//                      printf("a[%ld]=%d, b[%ld]=%d; \n", k, a[k], k, b[k]);
			k++;
		}
		numbernn++;
	} //For forth rows: f[12][ab]+f[12][ba]<=1
//      printf("k=%ld;\n", k);

/*...................................................................................................*/

	int MatrixVar = (2*m*n)+(mm*n*(2*(n-1)+2))+(mm*nn); //Number of matrix attributes
//	printf("MatrixVar= %d;\n", MatrixVar);

	glp_load_matrix(lp, MatrixVar, a, b, ar); //load matrix with matrix attributes and three arrays
	glp_simplex(lp, NULL); //We need simplex method first to solve mip problems
	glp_intopt(lp, NULL); //Solve mip Problem
	z = glp_mip_obj_val(lp); //return object value of problem
//	printf("z=%f;\n",z);

	int o = 1;
	for ( int i = 1; i < m+1; i = i+1 ) {
		for ( int j = 1; j < n+1; j = j+1 ) {
			x[j][i] = glp_mip_col_val(lp, o);
			if (x[j][i]) {
				printf("x[%d][%d]=%d;\t", j, i, x[j][i]);
			}
			o = o + 1;
		}
		printf("\n");
	} //get column values for nodes

	for ( int i = 1; i < m+1; i++) {
		for ( int j = 1; j < m+1; j++) {
			if ( i == j ) {

			}
			else {
				for ( int l=1; l < n+1; l++) {
					for ( int d=1; d < n+1; d++) {
						if ( l == d ) {

						}
						else {
							f[l][d][i][j] = glp_mip_col_val(lp, o);
							if (f[l][d][i][j]) {
								printf("f[%d][%d][%d][%d]=%f;\n", l, d, i, j, f[l][d][i][j]);
							}
							o++;
						}
					}
				}
			}
			printf("\n");
		}
	 } //get column values for links

	glp_delete_prob(lp);
	glp_free_env();
	return 0;
}
