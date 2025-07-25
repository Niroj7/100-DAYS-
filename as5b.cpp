#include <fstream>
#include <iostream>
using namespace std;
int main()
{
        char ch;
   int cnt=0;
   double amount=0.0;
const int SIZE = 4;
int accNo,countDeposits=0,cntWithdrawls=0; // flag 
    double bal,amt=.0,totInterest=0.0,totDeposit=0.0,totWithdrawn=0.0;
    double serviceCharges=0.0;

ifstream dataIn;
   dataIn.open("bankdata.txt");// data is stored in this file.
  
if(dataIn.fail())// checks whether or not file name is valid or not
{
   cout<<"** File Not Found **";// if file is not found then it outputs
return 1;
}
else
{
dataIn>>accNo>>bal;
amount=bal;

while(dataIn>>ch>>amt)// reads the data from the file{
switch(ch)  {
    case 'W':
        if(bal-amt>=0)
        {
            bal-=amt;
            totWithdrawn+=amt;
            cntWithdrawls++;
            cnt++; }
        break;
    case 'D':
        bal+=amt;
        countDeposits++;
        totDeposit+=amt;
        cnt++;
        break;
    case 'I':
        bal+=amt;
        totInterest+=amt;
        break;}
     
   if(cnt> 5 && bal<1000) {
      serviceCharges+=0.25;  }
dataIn.close();
cout<<"Account Number :"<<accNo<<endl;
cout<<" beginning of the month: $"<<amount<<endl;
cout<<"interest paid by the bank: $"<<totInterest<<endl;
cout<<"Total amount of deposit :$"<<totDeposit<<endl;
cout<<"Number of deposit :"<<countDeposits<<endl;
cout<<"Total amount of withdrawl :$"<<totWithdrawn<<endl;
cout<<"Number of withdrawls :"<<cntWithdrawls<<endl;
cout<<"Service charges :$"<<serviceCharges<<endl;
  
}
return 0;
}