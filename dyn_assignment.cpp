//Dynamic Assignment load balancing
#include<stdio.h>
#include<stdlib.h>
#include<mpi.h>
#include<sys/time.h>


void printtime(int rank,char *text, struct timeval t1, struct timeval t2)
{
   long long l;
   long secs, usecs;
   l=t2.tv_sec*1000000+t2.tv_usec-(t1.tv_sec*1000000+t1.tv_usec);
   secs=l/1000000;
   usecs=l%1000000;

   printf("%d: %s: (%ld:%ld -> %ld:%ld (%ld:%ld)\n",rank,text,t1.tv_sec,t1.tv_usec,t2.tv_sec,t2.tv_usec,secs,usecs);
}

int main(int argc, char *argv[])
{
    int rank, size;


   int disp_width, disp_height;
   float real_min, real_max, imag_min, imag_max, scale_real, scale_imag;

   disp_width=atoi(argv[1]);
   disp_height=atoi(argv[2]);
   real_min= atof(argv[3]);
   real_max=atof(argv[4]);
   imag_min=atof(argv[5]);
   imag_max=atof(argv[6]);

  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
 // printf("no. of processors=%d\n",size);

  //MASTER
  if(rank==0)
{
    MPI_Status status;
   // printf("master rank is %d\n",rank);
    int r=0;
    int** pic;
    int i,j,x,y;
    struct timeval t1,t2,t3,t4,t5,t6;
   int done=1;
    int deadWorkers=0;
    int batch=100;
    int dispRows[disp_width*batch+1];
    int slave;
    int rowNo; 
    FILE *f;
    char str[256];
    int map[3][257];
    MPI_Request request;
    pic= (int**)malloc(sizeof(int*)*disp_width);
    for(x=0;x<disp_width;x++)
       pic[x]=(int*)malloc(sizeof(int)*disp_height);
    gettimeofday(&t1,NULL);
   //MPI_Send(&r,1,MPI_INT,1,0,MPI_COMM_WORLD);
  // MPI_Wait(&request,&status);
        for(i=1;i<size;i++)
        {
         MPI_Send(&r,1,MPI_INT,i,0,MPI_COMM_WORLD);
        r+=batch;
        }


 while(done)
      {
       if(deadWorkers!=size-1)
         {
           gettimeofday(&t3,NULL);
           MPI_Recv(&dispRows,disp_width*batch+1,MPI_INT,MPI_ANY_SOURCE,1,MPI_COMM_WORLD, &status);
          gettimeofday(&t4,NULL);
          slave=status.MPI_SOURCE;
          printtime(slave,"comm time",t3,t4);

           rowNo=dispRows[0];
           for(x=0;x<batch;x++)
           {
           for(y=0;y<disp_width;y++)
           {
              pic[rowNo][y]=dispRows[y+x*disp_width+1];
           }
              rowNo++;
           }
           slave=status.MPI_SOURCE;
           if(r==disp_height)
             {
               for(i=1;i<size;i++)
                 {
                   MPI_Send(0,0,MPI_INT,i,2,MPI_COMM_WORLD);
                   deadWorkers++;
                 }
             }
           else
             {
               MPI_Send(&r,1,MPI_INT,slave,0,MPI_COMM_WORLD);
               r+=batch;
             }
        //printf("r is %d\n",r);

        }
        if(deadWorkers==size-1)
        done=0;

      }
        gettimeofday(&t2,NULL);
        printtime(rank,"compute time",t1,t2);
        f=fopen(argv[7],"r");
        for(i=0;i<256;i++)
        {
          fgets(str,1000,f);
          sscanf(str,"%d %d %d",&(map[0][i]),&(map[1][i]),&(map[2][i]));
        }

        fclose(f);
        f=fopen(argv[8],"w");
        fprintf(f,"P3\n%d %d\n255\n",disp_width,disp_height);
        gettimeofday(&t5,NULL);
        for(y=0;y<disp_height;y++)
        {
        for(x=0;x<disp_width;x++)
        {
          fprintf(f,"%d %d %d ",map[0][pic[x][y]],map[1][pic[x][y]],map[2][pic[x][y]]);
        }
          fprintf(f,"\n");
        }
        gettimeofday(&t6,NULL);
        fclose(f);
        printtime(rank,"io time",t5,t6);
}

else
{
int max=256,x,y,k;
float temp,temp2,lengthsq,zreal,creal,zimag,cimag;
float scale_real=(real_max-real_min)/disp_width;
float scale_imag=(imag_max-imag_min)/disp_height;
int batch=100;
int dispRow[disp_width*batch+1];
int r=0;
int count;
struct timeval t3,t4;

MPI_Status status;
int slave;
int done=0;
MPI_Comm_rank(MPI_COMM_WORLD,&slave);
//MPI_ISend()
MPI_Recv(&r,1,MPI_INT,0,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
while(!done)
{
   //check if terminate signal is received
    done=(status.MPI_TAG==2);
    dispRow[0]=r;
    for(y=r;y<batch+r;y++)
      {
        k=y-r;
        for(x=0;x<disp_width;x++)
         {
           creal=real_min+((float)x*scale_real);
           cimag=imag_min+((float)y*scale_imag);

           zreal=0;
           zimag=0;
           temp=0;
           temp2=0;
          count=0;
           do{
               temp=temp-temp2+creal;
               zimag=2*zreal*zimag+cimag;
                zreal=temp;
                temp=zreal*zreal;
                temp2=zimag*zimag;
                lengthsq=temp+temp2;
                count++;
             }while((lengthsq<4.0)&&(count<max));
                dispRow[x+k*disp_width+1]=count;       
         }
      }

        MPI_Send(&dispRow,disp_width*batch+1,MPI_INT,0,1,MPI_COMM_WORLD);


         MPI_Recv(&r,1,MPI_INT,0,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
}

}
MPI_Finalize();
}