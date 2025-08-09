
//NIROJ koirala






import java.util.Scanner;    // HEADER 

public class Main {
    public static void main(String args[]){  
        
        
        Scanner scanner = new Scanner(System.in);       // FOR A KEYBOARD INPUT
        int num;
                    int score = Integer.MIN_VALUE;       
                    
        System.out.print("Enter the number of students : ");  // Asks user for input
        num = scanner.nextInt();
        System.out.print("Enter "+num+" scores: ");          // promts user for input
        
        System.out.println();      // prints empty line shown in Q
        
        
        int array[] = new int[num];
        
        for(int i = 0;i<num;i++){               // CREATION OF ARRAY AND USING FOR LOOP
            array[i] = scanner.nextInt();
            if(score < array[i]){
                score = array[i];
            }
        }
        

        for(int i = 0;i<num;i++){
            System.out.print("Student# "+(i+0)+" score is "+array[i]+" and grade is "); // SCREEN PROMTS
            
            if(array[i] >=  score - 5){
                System.out.println("A");
            }                                         // CHECKING OF EACH CONDITION
            else if(array[i] >= score - 10){
                System.out.println("B");
            }
            else if(array[i] >=  score - 15){
                System.out.println("C");
            }
            else if(array[i] >= score - 20){
                System.out.println("D");
            }
            else{
                System.out.println("F");      // ALSO KNOWN AS DEFAULT OR UNMATCING CONDITION
            }
        }

    }
}