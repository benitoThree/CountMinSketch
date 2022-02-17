mod time_series;

use time_series::CountMinSketch;
fn main() {
    const A: usize = 10;
    const B: usize = 1000;
    let mut cms: CountMinSketch<A, B> = CountMinSketch::new();
    let x_arr = [1,2,3,4,5,6,7,8,9,10];
    cms.batch_index(&x_arr);
    let query_results = cms.batch_query(&x_arr);
    println!("Results: {:?}", query_results);
}
