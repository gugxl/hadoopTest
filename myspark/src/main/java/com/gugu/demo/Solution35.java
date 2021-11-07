package com.gugu.demo;

/**
 * @author Administrator
 * @Classname Solution35
 * @Description TODO
 * @Date 2021/11/7 17:54
 */
public class Solution35 {
    public static void main(String[] args) {
        int []nums = new int[]{1,3,5,6};
        int target = 5;
        System.out.println(searchInsert(nums, target));
    }
    public static int searchInsert(int[] nums, int target) {
        int leftInx = 0;
        int rightInx = nums.length - 1;
        while (leftInx <= rightInx){
            int mid = (rightInx+leftInx)/2;
            if (nums[mid]==target){
                return (rightInx+leftInx)/2;
            }else if (nums[mid] < target){
                leftInx = mid+1;
            }else {
                rightInx = mid-1;
            }
        }
        return leftInx;
    }
}
