#include <stdio.h>
#include <stdlib.h>
  
  int main(int argc, char *argv[])
  {
    int frameNum;
    frameNum = 0;
    if (argc >= 2) {
      frameNum = atoi(argv[1]);
    }
    printf("Hello, world! From frameNum = %d\n", frameNum);
    return 0;
  }

