package com.mac286.practice;

import java.util.Stack;
import java.util.Vector;

public class Problem {
    public static void main(String[] args) {
        // create a stack and fill it with 100 random numbers
        Stack<Integer> stack = new Stack<>();
        for (int i = 0; i < 100; i++) {
            int number = (int) (Math.random() * 200) - 100; // generates random number between -100 and 100
            stack.push(number);
        }
        
      //Using one genericVector object, we want to  reorganize elements in the stack so that 
      		//all negative numbers go to the bottom and the positive numbers to the top.
      		 
      		  Vector <Integer> genericVector = new Vector <>();
      			while (!stack.isEmpty()) {
      				int number = stack.pop();
      				if ( number< 0) {
      				 genericVector.add(0, number);   // adds negative numbers to the bottom
      				}
      				else {
      				 genericVector.add(number);    // adds positive numbers to the top
      				}
      			}
      		

        
        // print the elements of the vector
      	System.out.println("The elements after reorganizing are: ");
        for (int i = 0; i < genericVector.size(); i++) {
            System.out.println(genericVector.get(i));
				
			}

        }
    }
