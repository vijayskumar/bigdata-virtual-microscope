#include "ocvmquery.h"
#include "ocvm.h"

using namespace std;

char * appname = NULL;
void usage()
{
    printf(
        "\n\n\nusage: %s\n"
        "   [-partialps] # query from partial prefix sum \n"
        "   <prefix_sum_descriptor_file>\n"
        "   <-p | -g>\n"
        "   # for original image coordinates and grid coordinates respectively\n"
        "   <query_file>\n"
        "    # The file format for <query_file> is for each vertex:\n"
        "    <point_x_coordinate> <point_y_coordinate> <point_z_coordinate>\n"
        "    ...\n"
        "        OR\n"
        "    <grid_x_coordinate> <grid_y_coordinate> <grid_z_coordinate>\n"
        "    ...\n",
        appname);
    exit(EXIT_FAILURE);
}


int main(int argc, char * argv[])
{
    int i, j, k;
    ImageDescriptor prefix_sum_descriptor;
    int x, y, z;
    int cell_rows_per_chunk;
    int cell_columns_per_chunk;
    bool partial_prefix_sum = false;
    int num_queries;
    double starttime, endtime;
    int total_workload = 0, total_files = 0;

    if (((argc-1) == 0) || (!strcmp(argv[1],"-h"))) {
        appname = argv[0];
        usage();
    }

    // printout arguments
    cout << "executing: ";
    for (i = 0; i < argc; i++) {
        if (i) {
            cout << " ";
        }
        cout << argv[i];
    }
    cout << endl;

    starttime = dcmpi_doubletime();
    if (!strcmp(argv[1],"-partialps")) {
        partial_prefix_sum = true;
        dcmpi_args_shift(argc,argv);
    }
    prefix_sum_descriptor.init_from_file(argv[1]); 

    std::vector<std::string> prefix_sum_hosts;
    std::vector<std::string> prefix_sum_hosts_set;
    for (int u = 0; u < (int)prefix_sum_descriptor.get_num_parts(); u++) {
        const std::string & hn = prefix_sum_descriptor.parts[u].hostname;
        prefix_sum_hosts.push_back(hn);
        if (std::find(prefix_sum_hosts_set.begin(),
                      prefix_sum_hosts_set.end(),
                      hn) == prefix_sum_hosts_set.end()) {
            prefix_sum_hosts_set.push_back(hn);
        }
    }
    cell_rows_per_chunk = prefix_sum_descriptor.pixels_y / prefix_sum_descriptor.chunks_y;
    cell_columns_per_chunk = prefix_sum_descriptor.pixels_x / prefix_sum_descriptor.chunks_x; 
    if (!fileExists(argv[3])) {
        std::cerr << "ERROR: "
                  << argv[3] << " does not exist"
                  << std::endl << std::flush;
        exit(1);
    }

    FILE * f = fopen(argv[3], "r");
    if (!f) {
        std::cerr << "ERROR: opening file " << argv[3]
                  << " at " << __FILE__ << ":" << __LINE__
                  << std::endl << std::flush;
        exit(1);
    }

    int chunk_x, chunk_y;
    off_t chunk_offset;
    std::string hostname;
    std::string filename;
    std::vector< std::string> cellSize;
    std::string & extra = prefix_sum_descriptor.extra;
    cellSize = str_tokenize(extra);

    std::vector< std::string> all_chunk_sums;
    std::map< std::string, std::string > chunk_sum_mapping; 
    if (partial_prefix_sum) {
        all_chunk_sums = str_tokenize(cellSize[5], ":");
        for (i = 0; i < (int)all_chunk_sums.size(); i++) {
            std::vector< std::string> chunk_sum = str_tokenize(all_chunk_sums[i], "/");
            chunk_sum_mapping[chunk_sum[0]] = chunk_sum[1];
        }
    }

    typedef std::map< std::string, std::map< std::string, std::vector< off_t> > >
        host_chunk_offset_mapping ;
    host_chunk_offset_mapping host_chunk_offset;
    typedef std::map< std::string, std::map< std::string, std::string > >
        host_query_offset_mapping ;
    host_query_offset_mapping host_query_offset;

    std::string s = file_to_string(argv[3]);
    fclose(f);
    std::vector< std::string> lines = str_tokenize(s, "\n");
    num_queries = lines.size();
    std::vector< ocvm_sum_integer_type> prefix_sum_results[3];
    for (int i=0; i < num_queries; i++) {
        prefix_sum_results[0].push_back((ocvm_sum_integer_type) 0);
        prefix_sum_results[1].push_back((ocvm_sum_integer_type) 0);
        prefix_sum_results[2].push_back((ocvm_sum_integer_type) 0);
    }


    for (i = 0; i < (int)lines.size(); i++) {
        std::vector< std::string> tokens = str_tokenize(lines[i]);
        if (tokens.size() == 3) {
            x = Atoi(tokens[0]);  
            y = Atoi(tokens[1]);  
            z = Atoi(tokens[2]);  
            if (!strcmp(argv[2],"-p")) {
                x = x / Atoi(cellSize[1]);
                y = y / Atoi(cellSize[3]);
            }

            prefix_sum_descriptor.pixel_to_chunk(x, y, chunk_x, chunk_y);
            if (!partial_prefix_sum) {
                ImageCoordinate ic(chunk_x, chunk_y, z);
	   
                hostname = prefix_sum_descriptor.get_part(ic).hostname;
                filename = prefix_sum_descriptor.get_part(ic).filename;
	   
                off_t row_offset = (y > 0) ? ((y) % cell_rows_per_chunk) : 0;
                chunk_offset = row_offset * cell_columns_per_chunk * sizeof(ocvm_sum_integer_type) +
                    (x % cell_columns_per_chunk) * sizeof(ocvm_sum_integer_type);
                //cout << "Chunk offset: " << chunk_offset << endl;

                if (host_chunk_offset.count(hostname) == 0) {
                    host_chunk_offset[hostname] =
                        std::map< std::string, std::vector< off_t> >();
                }
                std::map< std::string, std::vector< off_t> > &
                    chunk_offset_mapping = host_chunk_offset[hostname];
                if (chunk_offset_mapping.count(filename) == 0) {
                    chunk_offset_mapping[filename] = std::vector< off_t>();
                    total_files++;
                }
                chunk_offset_mapping[filename].push_back(chunk_offset);
            }
            else {
                for (k = 0; k < chunk_y; k++) {
                    j = chunk_x;
                    ImageCoordinate ic(j, k, z);
	   
                    hostname = prefix_sum_descriptor.get_part(ic).hostname;
                    filename = prefix_sum_descriptor.get_part(ic).filename;

                    if (k == 0) {
                        chunk_offset = (x % cell_columns_per_chunk) * sizeof(ocvm_sum_integer_type);
                    }
                    //cout << "\tRight column chunk " << j << " " << k << " " << z << " offset: " << chunk_offset << endl;


                    if (host_chunk_offset.count(hostname) == 0) {
                        host_chunk_offset[hostname] =
                            std::map< std::string, std::vector< off_t> >();
                    }
                    std::map< std::string, std::vector< off_t> > &
                        chunk_offset_mapping = host_chunk_offset[hostname];
                    if (chunk_offset_mapping.count(filename) == 0) {
                        chunk_offset_mapping[filename] = std::vector< off_t>();
                        total_files++;
                    }
                    chunk_offset_mapping[filename].push_back(chunk_offset);
			
                    if (host_query_offset.count(hostname) == 0) {
                        host_query_offset[hostname] =
                            std::map< std::string, std::string >();
                    }
                    std::map< std::string, std::string > &
                        query_offset_mapping = host_query_offset[hostname];
                    if (query_offset_mapping.count(filename) == 0) {
                        query_offset_mapping[filename] = "";
                    }
                    query_offset_mapping[filename] = query_offset_mapping[filename] + tostr(i) + ","; 
                }
                for (j = 0; j <= chunk_x; j++) {
                    k = chunk_y;
                    ImageCoordinate ic(j, k, z);

                    hostname = prefix_sum_descriptor.get_part(ic).hostname;
                    filename = prefix_sum_descriptor.get_part(ic).filename;

                    if (j == chunk_x) {
                        off_t column_offset = (x % cell_columns_per_chunk) * sizeof(ocvm_sum_integer_type);
                        if ((y % cell_rows_per_chunk) == cell_rows_per_chunk-1) {
                            chunk_offset = column_offset;
                        }
                        else {
                            chunk_offset = 3 * cell_columns_per_chunk * sizeof(ocvm_sum_integer_type) +
                                (y % cell_rows_per_chunk) * cell_columns_per_chunk * sizeof(ocvm_sum_integer_type) + 
                                column_offset;
                        }
                        //cout << "\tCorner chunk " << j << " " << k << " " << z << " offset: " << chunk_offset << endl;
                    }
                    else if (j == 0) {
                        if ((y % cell_rows_per_chunk) == cell_rows_per_chunk-1) {
                            chunk_offset = (cell_columns_per_chunk - 1) * sizeof(ocvm_sum_integer_type);
                        }
                        else {
                            chunk_offset = 3 * cell_columns_per_chunk * sizeof(ocvm_sum_integer_type) +
                                (y % cell_rows_per_chunk) * cell_columns_per_chunk * sizeof(ocvm_sum_integer_type) + 
                                (cell_columns_per_chunk - 1) * sizeof(ocvm_sum_integer_type);
                        }
                    }
                    //cout << "\tBottom row chunk " << j << " " << k << " " << z << " offset: " << chunk_offset << endl;


                    if (host_chunk_offset.count(hostname) == 0) {
                        host_chunk_offset[hostname] =
                            std::map< std::string, std::vector< off_t> >();
                    }
                    std::map< std::string, std::vector< off_t> > &
                        chunk_offset_mapping = host_chunk_offset[hostname];
                    if (chunk_offset_mapping.count(filename) == 0) {
                        chunk_offset_mapping[filename] = std::vector< off_t>();
                        total_files++;
                    }
                    chunk_offset_mapping[filename].push_back(chunk_offset);

                    if (host_query_offset.count(hostname) == 0) {
                        host_query_offset[hostname] =
                            std::map< std::string, std::string >();
                    }
                    std::map< std::string, std::string > &
                        query_offset_mapping = host_query_offset[hostname];
                    if (query_offset_mapping.count(filename) == 0) {
                        query_offset_mapping[filename] = "";
                    }
                    query_offset_mapping[filename] = query_offset_mapping[filename] + tostr(i) + ",";
                }
                if (chunk_x > 0 && chunk_y > 0) {
                    j = chunk_x - 1; k = chunk_y - 1;
                    ImageCoordinate ic(j, k, z);
	
                    hostname = prefix_sum_descriptor.get_part(ic).hostname;
                    filename = prefix_sum_descriptor.get_part(ic).filename;

                    //cout << "\tComplete chunk " << j << " " << k << " " << z << " offset: " << chunk_offset << endl;
                    std::string chunk_string = tostr(j) + "," + tostr(k) + "," + tostr(z);
                    std::vector<std::string> complete_chunk_offset = str_tokenize(chunk_sum_mapping[chunk_string], ",");
                    for (int l = 0; l < (int)complete_chunk_offset.size(); l++) {
                        prefix_sum_results[l][i] += Atoi(complete_chunk_offset[l]);
                    }
                }
            }
        }
    }


    DCLayout layout;
    layout.use_filter_library("libocvmqueryfilters.so");
    DCFilterInstance console("<console>", "console1");
    layout.add(console);
    std::map<std::string, DCFilterInstance*> host_workload;
    host_chunk_offset_mapping::iterator it; 
    std::map< std::string, std::vector< off_t> >::iterator it2;
    std::vector< off_t>::iterator it3, it3b;
    for (it = host_chunk_offset.begin(); it != host_chunk_offset.end(); it++) {
        for (int u=0; u < (int)prefix_sum_hosts_set.size(); u++) {
            hostname = prefix_sum_hosts_set[u];
            if (!strcmp(it->first.c_str(), hostname.c_str())) {
                if (host_workload.count(hostname) == 0) {
                    host_workload[hostname] = new DCFilterInstance(
                        "ocvmqueryhandler","query_handler_" + tostr(u));
                    host_workload[hostname]->bind_to_host(hostname);
                    host_workload[hostname]->set_param("myhostname",hostname);
                    layout.add(host_workload[hostname]); 
                    layout.add_port(host_workload[hostname], "0", &console, "0");
                    host_workload[hostname]->set_param("my_workload", "");
                    host_workload[hostname]->set_param("color_offset", tostr(cell_rows_per_chunk * cell_columns_per_chunk * sizeof(ocvm_sum_integer_type)));
                    host_workload[hostname]->set_param("num_columns", tostr(cell_columns_per_chunk * sizeof(ocvm_sum_integer_type)));
                    host_workload[hostname]->set_param("partial", tostr(partial_prefix_sum));
                }

                std::map< std::string, std::vector< off_t> > &
                    chunk_offset_mapping = it->second;
                std::map< std::string, std::string > &
                    query_offset_mapping = host_query_offset[hostname];
                int query_unique_count = 0;
                for (it2 = chunk_offset_mapping.begin(); it2 != chunk_offset_mapping.end(); it2++) {
                    std::vector< off_t> & remote_chunk_offsets = it2->second;
                    std::vector< off_t> unique_remote_chunk_offsets; 
                    unique_remote_chunk_offsets  = std::vector< off_t>(); 
                    std::string tmp_workload = host_workload[hostname]->get_param("my_workload");
                    tmp_workload = tmp_workload + it2->first + " ";

                    if (!partial_prefix_sum) {
                        for (it3 = remote_chunk_offsets.begin(); it3 != remote_chunk_offsets.end(); it3++) {
                            tmp_workload = tmp_workload + tostr(*it3) + " ";
                            total_workload++;
                        }
                    }
                    else {
                        std::vector< std::string> query_unique = str_tokenize(query_offset_mapping[it2->first], ",");
                        query_offset_mapping[it2->first] = "";
                        int query_count = 0;
                        for (it3 = remote_chunk_offsets.begin(); it3 != remote_chunk_offsets.end(); it3++) {
                            bool repeat = false;
                            for (it3b = unique_remote_chunk_offsets.begin(); it3b != unique_remote_chunk_offsets.end(); it3b++) {
                                if(*it3b == *it3) {
                                    repeat = true;
                                    break;
                                }
                            }
                            if (!repeat) {
                                unique_remote_chunk_offsets.push_back(*it3); 
                                query_unique_count++;
                                query_offset_mapping[it2->first] += query_unique[query_count] + ",";
                            }
                            else {
                                query_offset_mapping[it2->first] += query_unique[query_count] + "-" + tostr(query_unique_count-1) + ",";
                            }
                            query_count++;
                        }
                        for (it3 = unique_remote_chunk_offsets.begin(); it3 != unique_remote_chunk_offsets.end(); it3++) {
                            tmp_workload = tmp_workload + tostr(*it3) + " ";
                            total_workload++;
                        }
                    }
                    tmp_workload = tmp_workload + "\n";
                    host_workload[hostname]->set_param("my_workload", tmp_workload);
                }
                break;
            }
        }
    }
    endtime = dcmpi_doubletime();
    double setuptime = endtime - starttime;
    starttime = dcmpi_doubletime();
    DCFilter * consoleFilter = layout.execute_start();
    endtime = dcmpi_doubletime();
    double execute_start_time = endtime - starttime;

    starttime = dcmpi_doubletime();
    int query_count = 0;
    std::map<std::string, DCFilterInstance*>::iterator it4;
    host_query_offset_mapping::iterator it5;
    std::map< std::string, std::string >::iterator it6;
    std::vector< std::string> query_keys;
    for (it4 = host_workload.begin(); it4 != host_workload.end(); it4++) {
        DCBuffer * inb = consoleFilter->read("0");
        std::string source;
        inb->Extract(&source);
        ocvm_sum_integer_type * array = (ocvm_sum_integer_type *)inb->getPtrExtract();
	
        if (!partial_prefix_sum) {
            for (int i = 0; i < (int)((inb->getUsedSize() - source.length())/sizeof(ocvm_sum_integer_type)); i+=3) {
                prefix_sum_results[0][query_count] = array[i];
                prefix_sum_results[1][query_count] = array[i+1];
                prefix_sum_results[2][query_count++] = array[i+2];
            }
        }
        else { 
            for (it5 = host_query_offset.begin(); it5 != host_query_offset.end(); it5++) {
                if (it5->first.c_str() == source) {
                    std::map< std::string, std::string > &
                        query_offset_mapping = it5->second;
                    for (it6 = query_offset_mapping.begin(); it6 != query_offset_mapping.end(); it6++) {
                        std::vector< std::string> query_key = str_tokenize(it6->second, ",");
                        for (int i = 0; i < (int)query_key.size(); i++) query_keys.push_back(query_key[i]);
                    }		      
                    std::vector< std::string> query_unique;
                    j = 0;
                    for (int i = 0; i < (int)((inb->getUsedSize() - source.length())/sizeof(ocvm_sum_integer_type)); i+=3) {
                        while (j < (int)query_keys.size()) {
                            query_unique = str_tokenize(query_keys[j], "-");
                            if (query_unique.size() > 1) {
                                j++;
                                prefix_sum_results[0][Atoi(query_unique[0])] += array[Atoi(query_unique[1])*3];
                                prefix_sum_results[1][Atoi(query_unique[0])] += array[Atoi(query_unique[1])*3 + 1];
                                prefix_sum_results[2][Atoi(query_unique[0])] += array[Atoi(query_unique[1])*3 + 2];
                            } 
                            else
                                break;
                        }
                        prefix_sum_results[0][Atoi(query_keys[j])] += array[i];
                        prefix_sum_results[1][Atoi(query_keys[j])] += array[i+1];
                        prefix_sum_results[2][Atoi(query_keys[j])] += array[i+2];
                        j++;
                    }
                    while (j < (int)query_keys.size()) {
                        query_unique = str_tokenize(query_keys[j], "-");
                        if (query_unique.size() > 1) {
                            j++;
                            prefix_sum_results[0][Atoi(query_unique[0])] += array[Atoi(query_unique[1])*3];
                            prefix_sum_results[1][Atoi(query_unique[0])] += array[Atoi(query_unique[1])*3 + 1];
                            prefix_sum_results[2][Atoi(query_unique[0])] += array[Atoi(query_unique[1])*3 + 2];
                        }
                    }
                    query_keys.clear();
                    break;
                }
            }
        }
        inb->consume();
    }

#ifdef DEBUG
    cout << "BLUE channel: ";
    for (int i=0; i < num_queries; i++) cout << prefix_sum_results[0][i] << " ";
    cout << endl;
    cout << "GREEN channel: ";
    for (int i=0; i < num_queries; i++) cout << prefix_sum_results[1][i] << " ";
    cout << endl;
    cout << "RED channel: ";
    for (int i=0; i < num_queries; i++) cout << prefix_sum_results[2][i] << " ";
    cout << endl;
#endif

    int rc = layout.execute_finish();
    endtime = dcmpi_doubletime();
    double readcombtime = endtime - starttime;
    cout << "SET UP TIME: " << setuptime << endl;
    cout << "EXECUTE_START TIME: " << execute_start_time << endl;
    cout << "READ and COMBINATION TIME: " << readcombtime << endl;
    cout << "OVERALL TIME: " << setuptime + execute_start_time + readcombtime
         << endl;
    cout << "#QUERY HANDLER FILTERS: " << host_chunk_offset.size() 
         << endl;
    cout << "SIZE OF WORKLOAD: " << total_workload << endl;
    cout << "#PREFIX SUM FILES INVOLVED: " << total_files << endl;
    return rc;
}
