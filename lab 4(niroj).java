
import java.util.Scanner;

public class Main
{
    public static void sumofMatrix(int[][] a, int[][] b){ // declaring two arays
        
        int[][] c = new int[3][3];
        
        
        for(int i = 0; i < a.length; i++){
		    for(int j = 0; j < b.length; j++){ // nested for loop
		      c[i][j] = a[i][j] + b[i][j];  
		    }
		}
        
        // displaying the sum.
        System.out.println("The sum of matrix 1 and matrix 2 is: "); //promts in the screen
        for(int i = 0; i < a.length; i++){
		    for(int j = 0; j < b.length; j++){
		      System.out.print(c[i][j]+ " ");
		    }
		    System.out.println();  
		}
    }
    
	public static void main(String[] args) {
		
	
		int[][] matrix1 = new int[3][3];
		int[][] matrix2 = new int[3][3];   // Declares array.
		
		Scanner sc = new Scanner(System.in);
		
		// read the elements into matrix 1 and matrix 2
		System.out.println("Enter the elments of Matrix 1: "); // promts in the sceen
		for(int i = 0; i < matrix1.length; i++){
		    for(int j = 0; j < matrix1.length; j++){ // using the nested for loop
		      matrix1[i][j] = sc.nextInt();  
		    }
		}
		
		System.out.println("Enter the elments of Matrix 2: "); // promts in screen
		for(int i = 0; i < matrix2.length; i++){
		    for(int j = 0; j < matrix2.length; j++){ // // using the nested for loop
		      matrix2[i][j] = sc.nextInt();  
		    }
		}
		
	
		sumofMatrix(matrix1,matrix2); // function call
	}
}
