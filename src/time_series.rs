use crossbeam::epoch::Atomic;
use rand;
use std::{thread};
use std::sync::{Arc};
use std::sync::atomic::{AtomicU32, Ordering};
use crossbeam::thread::scope;
use array_init::array_init;

pub struct CountMinSketch<const A: usize, const B: usize> {
    arr: [[AtomicU32; B]; A],
    hash_constants: [(usize, usize); A],
}

impl <const A: usize, const B: usize> CountMinSketch<A, B> {
    pub fn new() -> Self {
        Self { 
            arr: array_init(|_| array_init(|_| AtomicU32::new(0))), 
            hash_constants: [(rand::random::<usize>(), rand::random::<usize>()); A], 
        }
    }

    pub fn batch_index<const X: usize>(self: &mut Self, x_arr: &[u32; X]) {
        let self_arc = Arc::new(self);
        scope(|scope| {
            // let self_arr = Arc::new(self.arr);
            x_arr.map(|x| {
                let self_arc = Arc::clone(&self_arc);
                scope.spawn(move |_| {
                    let indices = self_arc.getIndices(x);
                    for (i, &idx) in indices.iter().enumerate() {
                        self_arc.arr[i][idx].fetch_add(1, Ordering::Relaxed);
                    }
                })
            }).map(|handle| {
                handle.join().unwrap()
            })
        }).expect("Threads failed.");
    }

    pub fn batch_query<const X: usize>(self: &Self, x_arr: &[u32;X]) -> [u32;X] {
        let self_arc = Arc::new(self);
        scope(|scope| {
            // let self_arr = Arc::new(self.arr);
            x_arr.map(|x| {
                let self_arc = Arc::clone(&self_arc);
                scope.spawn(move |_| {
                    let indices = self_arc.getIndices(x);
                    indices.iter().enumerate().map(|(i, &idx)| {
                        self_arc.arr[i][idx].load(Ordering::Relaxed)
                    }).reduce(u32::min).unwrap()
                })
            }).map(|handle| {
                handle.join().unwrap()
            })
        }).unwrap()
    }

    fn getIndices(self: & Self, x: u32) -> [usize; A] {
        self.hash_constants.map(|(a, b)| (a.wrapping_mul(x as usize).wrapping_add(b)) % B)
    }
}



// TODO Lets work on an interface
