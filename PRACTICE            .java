import java.util.Scanner;

 

public class Main {

    //creating a double array

    public static double[][] multiplyMatrix(double[][] a, double[][] b)

    {

        double[][] result = new double[a.length][b[0].length];

        for (int i = 0; i < result.length; ++i) { // using nested for loop

            for (int j = 0; j < result[i].length; ++j) {

                double c = 0;

                for (int k = 0; k < a[i].length; ++k) {

                    c += (a[i][k] * b[k][j]);

                }

                result[i][j] = c;

            }

        }

        return result;

    }

 

    public static void main(String[] args) {

        Scanner in = new Scanner(System.in);

        double[][] matrix1 = new double[3][3];

        double[][] matrix2 = new double[3][3];

 

        System.out.print("Enter matrix1: ");

        for (int i = 0; i < matrix1.length; ++i) { // using nested for loop

            for (int j = 0; j < matrix1[i].length; ++j) {

                matrix1[i][j] = in.nextDouble();

            }

        }

 

        System.out.print("Enter matrix2: ");

        for (int i = 0; i < matrix2.length; ++i) { // using nested for loop

            for (int j = 0; j < matrix2[i].length; ++j) {

                matrix2[i][j] = in.nextDouble();

            }

        }

        System.out.println();

        double[][] result = multiplyMatrix(matrix1, matrix2);

 

        System.out.println("The multiplication of the matrices is: "); // promts the msg in screen

        for (int i = 0; i < result.length; ++i) {

            for (int j = 0; j < result[i].length; ++j) {

                System.out.print(result[i][j] + " ");

            }

            System.out.println();

        }

    }

}