use rand;
use std::sync::atomic::{AtomicU32, Ordering};
use array_init::array_init;
use rayon::prelude::*;

pub struct CountMinSketch<const A: usize, const B: usize> {
    arr: [[AtomicU32; B]; A],
    hash_constants: [(usize, usize); A],
}

impl <const A: usize, const B: usize> CountMinSketch<A, B> {
    pub fn new() -> Self {
        Self { 
            arr: array_init(|_| {
                array_init(|_| AtomicU32::new(0))
            }), 
            hash_constants: [(rand::random(), rand::random()); A], 
        }
    }

    pub fn batch_index(self: &mut Self, x_arr: &Vec<u32>) {
        x_arr.par_iter().for_each(|&x| {
            self.get_indices(x).iter().enumerate().for_each(|(i, &idx)| {
                self.arr[i][idx].fetch_add(1, Ordering::Relaxed);
            });
        });
    }

    pub fn batch_query(self: &Self, x_arr: &Vec<u32>) -> Vec<u32> {
        x_arr.par_iter().map(|&x| {
            self.get_indices(x).iter().enumerate().map(|(i, &idx)| {
                self.arr[i][idx].load(Ordering::Relaxed)
            }).reduce(u32::min).unwrap()
        }).collect()
    }

    fn get_indices(self: & Self, x: u32) -> [usize; A] {
        self.hash_constants.map(|(a, b)| {
            (a.wrapping_mul(x as usize).wrapping_add(b)) % B
        })
    }
}



// TODO Lets work on an interface
