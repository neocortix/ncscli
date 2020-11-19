#include <iostream>
#include <string>
 
int main(int argc, char *argv[])
{
    int frameNum = 0;
    if (argc >= 2)
        frameNum = std::stoi(argv[1]);
    std::cout << "Hello, world! From frameNum = " << frameNum << std::endl;
    return 0;
}
