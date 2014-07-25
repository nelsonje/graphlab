#include <vector>
#include <string>
#include <fstream>

#include <mpi.h>

#include <graphlab.hpp>
#include <graphlab/rpc/buffered_exchange.hpp>
#include <graphlab/macros_def.hpp>

const uint64_t lcgM = 6364136223846793005UL;
const uint64_t lcgB = 1442695040888963407UL;

size_t me;
size_t size;

int64_t * my_chunk = NULL;

//typedef std::pair<size_t, size_t> data_t;
//typedef graphlab::buffered_exchange<data_t> exchange_t;
typedef graphlab::buffered_exchange<size_t> exchange_t;
exchange_t * exchange;

void increment( size_t index ) {
  my_chunk[index]++;
}

void recv_exchange( const bool tryit = false ) {
  graphlab::procid_t procid(-1);
  typename exchange_t::buffer_type buffer;
  while( exchange->recv( procid, buffer, tryit ) ) {
    foreach( const size_t offset, buffer ) {
      // foreach( const data_t d, buffer ) {
      // size_t offset = d.first;
      increment( offset );
    }
  }
}
    



int main(int argc, char** argv) {
  // Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  //global_logger().set_log_level(LOG_INFO);

  // Parse command line options -----------------------------------------------
  graphlab::command_line_options clopts("Giga-updates per second.");
  size_t sizeA = 1 << 30;
  size_t sizeB = 1 << 20;
  bool buffered = false;
  clopts.attach_option("sizeA", sizeA,
                       "Size of increment array.");
  clopts.attach_option("sizeB", sizeB,
                       "Size of random array.");
  clopts.attach_option("buffered", buffered,
                       "Use buffered exchange instead of raw RPC.");

  if(!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }


  dc.cout() << "Starting." << std::endl;

  me = dc.procid();
  size = dc.numprocs();

  exchange = new exchange_t( dc, size );
  
  size_t chunk_size = sizeA / size;
  size_t updates = sizeB / size;

  my_chunk = new int64_t[ chunk_size ];

  double start = MPI_Wtime();
  dc.barrier();
  if( buffered ) {
    const bool TRY_TO_RECV = true;
    const size_t TRY_RECV_MOD = 1000;

    //#pragma omp for
    for( int64_t i = updates * me; i < (updates * (me + 1)); ++i ) {
      uint64_t n = (uint64_t) i;
      uint64_t b = (lcgM * n + lcgB) % sizeA;
      size_t proc = b % size;
      size_t offset = b / size;
      //data_t d( offset, offset );
      //exchange->send( proc, d, 0 );
      exchange->send( proc, offset, 0 );
      //dc.remote_call( proc, &increment, offset );
      if(i % TRY_RECV_MOD == 0) recv_exchange(TRY_TO_RECV);
    }
    exchange->flush();
    recv_exchange();
  } else {
    for( int64_t i = updates * me; i < (updates * (me + 1)); ++i ) {
      uint64_t n = (uint64_t) i;
      uint64_t b = (lcgM * n + lcgB) % sizeA;
      size_t proc = b % size;
      size_t offset = b / size;
      dc.remote_call( proc, &increment, offset );
    }
  }
    dc.barrier();
  double end = MPI_Wtime();
  double runtime = end - start;
  double ups = sizeB / runtime;
  double upspercore = ups/size;
  
  dc.cout() << "Done. " << sizeB << " updates in "
            << runtime << " seconds: "
            << ups << " updates per second, "
            << upspercore << " updates per second per core."
            << std::endl;
  
  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
}


