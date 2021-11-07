package com.gugu.demo;

import java.util.Arrays;

class Solution27 {
    public static void main(String[] args) {
        int nums[] = new int[]{2};
        int val = 4;
        System.out.println(removeElement(nums, val));
        System.out.println(Arrays.toString(nums));
    }
    public static int removeElement(int[] nums, int val) {
        int j = nums.length - 1;

        for (int i = 0; i <= j; i++) {
            if (nums[i] == val){
                int tmp = nums[i];
                nums[i] = nums[j];
                nums[j] = tmp;
                i--;
                j--;
            }
        }
        return j+1;
    }
}