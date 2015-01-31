#include <boost/multiprecision/cpp_int.hpp>
#include <iostream>
#include <iterator>
#include <algorithm>

int main(int argc, char* argv[])
{
    using namespace boost::multiprecision;
    int batchSize = atoi(argv[1]);
    int biggest = atoi(argv[2]);
    
    for (std::string line; std::getline(std::cin, line);) {
        int start = atoi(line.c_str());

        for(int128_t a = start; a < start + batchSize; ++a){
            for(int128_t b = a+1; b < biggest; ++b){
                int128_t sum = (a*a*a) + (b*b*b);
                std::cout << sum << "\n";
            }
        }
    }
    
    return 0;
}
