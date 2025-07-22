#include <iostream>  //Niroj koirala
using namespace std ;
int main() {
    int digit,number;
    
    cout<< "enter r:";
    cin>>number;
    
    digit= number%10;
    cout<< digit <<'\n';
    
    number= number/10;
    digit=number%10 ;
    cout<<digit <<'\n';
    
    number= number/10 ;
    digit=number%10 ;
    cout<<digit <<'\n';

    number=number/10;
    digit=number%10;
    cout<<digit<<'\n';
    return 0;
}
