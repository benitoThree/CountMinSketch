#![feature(get_mut_unchecked)]
mod time_series;

use std::time::Instant;
use rayon::{self, ThreadPoolBuilder};
use time_series::{CountMinSketch};

fn main() {
    for _ in 0..10 {
        let pool = ThreadPoolBuilder::new().build().unwrap();
        pool.install(|| {
            const A: usize = 10;
            const B: usize = 100000;
            let mut cms: CountMinSketch<A, B> = CountMinSketch::new();
            
            let mut x_arr = vec![];
            let n_elems = 100000000;
            for j in 0..n_elems {
                x_arr.push(j);
            }
    
            let index_start = Instant::now();
            cms.batch_index(&x_arr);
            let index_elapsed = index_start.elapsed().as_millis();
    
            let query_start = Instant::now();            
            cms.batch_query(&x_arr);
            let query_elapsed = query_start.elapsed().as_millis();
            
            println!("Sum of elements {}", cms.sum());
            println!("Indexed in {} ms, queried in {} ms", index_elapsed, query_elapsed); 
        });
    }
}
