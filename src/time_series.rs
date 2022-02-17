use array_init::array_init;
use crossbeam;
use rand;
use rayon::prelude::*;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

pub struct CountMinSketch<const A: usize, const B: usize> {
    // Use Box to store in heap.
    // Use 2D array to avoid index arithmetic overhead
    // Unlike 2D vectors (vectors of vectors), this is contiguous in memory.
    // Array size uses constant generics so it is known at compile time.
    arr: Box<[[u32; B]; A]>,
    hash_constants: [(usize, usize); A],
}

impl<const A: usize, const B: usize> CountMinSketch<A, B> {
    pub fn new() -> Self {
        Self {
            arr: Box::new([[0; B]; A]),
            hash_constants: [(rand::random(), rand::random()); A],
        }
    }

    fn index_chunk(self: &mut Self, chunk: &[u32]) {
        chunk.iter().for_each(|&x| {
            self.get_indices(x)
                .iter()
                .enumerate()
                .for_each(|(i, &idx)| {
                    self.arr[i][idx] += 1;
                });
        });
    }

    pub fn batch_index(self: &mut Self, x_arr: &Vec<u32>) {
        crossbeam::scope(|s| {
            let arc = Arc::new(self);

            x_arr
                .chunks(x_arr.len() / rayon::current_num_threads())
                .for_each(|chunk| {
                    let mut arc = Arc::clone(&arc);
                    s.spawn(move |_| unsafe {
                        Arc::get_mut_unchecked(&mut arc).index_chunk(chunk);
                    });
                });
        })
        .err();
    }

    fn query_chunk(self: &Self, chunk: &[u32], result_slice: &mut Vec<u32>, start_idx: usize) {
        chunk.iter().enumerate().for_each(|(xi, &x)| {
            result_slice[start_idx + xi] = self.get_indices(x)
                .iter()
                .enumerate()
                .map(|(i, &idx)| self.arr[i][idx])
                .reduce(u32::min)
                .unwrap();
        });
    }

    pub fn batch_query(self: &Self, x_arr: &Vec<u32>) -> Vec<u32> {
        let mut results: Vec<u32> = Vec::with_capacity(x_arr.len());
        unsafe { results.set_len(x_arr.len()); }
        let chunk_size = x_arr.len() / rayon::current_num_threads(); 
        crossbeam::scope(|s| {
            let arc = Arc::new(self);
            let results_arc = Arc::new(&mut results);
            x_arr
                .chunks(chunk_size)
                .enumerate()
                .for_each(|(i, chunk)| {
                    let arc = Arc::clone(&arc);
                    let mut results_arc = Arc::clone(&results_arc);
                    s.spawn(move |_| {
                        let start = i * chunk_size;
                        unsafe {
                            let a = Arc::get_mut_unchecked(&mut results_arc);
                            Arc::as_ref(&arc).query_chunk(chunk, a, start);
                        }
                    });
                });
        }).err();
        results
    }

    pub fn sum(self: &Self) -> u32 {
        self.arr.iter().map::<u32, _>(|&arr| arr.iter().sum()).sum()
    }

    fn get_indices(self: &Self, x: u32) -> [usize; A] {
        self.hash_constants.map(|(a, b)| {
            (a.wrapping_mul(x as usize).wrapping_add(b)) % B
            // seahash::hash(&a.wrapping_mul(x as usize).wrapping_add(b).to_be_bytes()) as usize % B
        })
    }
}

pub struct CountMinSketchSafe<const A: usize, const B: usize> {
    // Use Box to store in heap.
    // Use 2D array to avoid index arithmetic overhead
    // Unlike 2D vectors (vectors of vectors), this is contiguous in memory.
    // Array size uses constant generics so it is known at compile time.
    arr: Box<[[AtomicU32; B]; A]>,
    hash_constants: [(usize, usize); A],
}

impl<const A: usize, const B: usize> CountMinSketchSafe<A, B> {
    pub fn new() -> Self {
        Self {
            arr: Box::new(array_init(|_| array_init(|_| AtomicU32::new(0)))),
            hash_constants: [(rand::random(), rand::random()); A],
        }
    }

    pub fn batch_index(self: &mut Self, x_arr: &Vec<u32>) {
        x_arr.par_iter().for_each(|&x| {
            self.get_indices(x)
                .iter()
                .enumerate()
                .for_each(|(i, &idx)| {
                    self.arr[i][idx].fetch_add(1, Ordering::Relaxed);
                });
        });
    }

    pub fn batch_query(self: &Self, x_arr: &Vec<u32>) -> Vec<u32> {
        x_arr
            .par_iter()
            .map(|&x| {
                self.get_indices(x)
                    .iter()
                    .enumerate()
                    .map(|(i, &idx)| self.arr[i][idx].load(Ordering::Relaxed))
                    .reduce(u32::min)
                    .unwrap()
            })
            .collect()
    }

    fn get_indices(self: &Self, x: u32) -> [usize; A] {
        self.hash_constants.map(|(a, b)| {
            (a.wrapping_mul(x as usize).wrapping_add(b)) % B
            // seahash::hash(&a.wrapping_mul(x as usize).wrapping_add(b).to_be_bytes()) as usize % B
        })
    }
}
