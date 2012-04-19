#include <dcmpi.h>

#include "ocvmquery.h"

using namespace std;

int ocvmqueryhandler::process()
{
    std::string my_workload = get_param("my_workload");
    off_t color_offset = (off_t)Atoi(get_param("color_offset"));
    off_t row_size = (off_t)Atoi(get_param("num_columns"));
    int partial = Atoi(get_param("partial"));
#ifdef DEBUG
//    cout << dcmpi_get_hostname() << " " <<  get_distinguished_name() << " workload: \n" << my_workload << endl;
#endif
    DCBuffer *outb;

    std::vector< std::string> lines = str_tokenize(my_workload, "\n");
    outb = new DCBuffer(lines.size()*sizeof(ocvm_sum_integer_type)*250);
    outb->Append(get_param("myhostname"));

    typedef std::map< std::string, std::map< off_t, std::string > >
        offset_value_mapping ;
    offset_value_mapping offset_value;

    for (int i = 0; i < (int)lines.size(); i++) {
        std::vector< std::string> tokens = str_tokenize(lines[i]);
        std::string filename = tokens[0];

        if (offset_value.count(filename) == 0) {
            offset_value[filename] =
                std::map< off_t, std::string >();
        }

        FILE * psumf = fopen(filename.c_str(),"r");
        if (!psumf) {
            std::cerr << "ERROR: opening file " << filename
                      << " on host " << dcmpi_get_hostname(true)
                      << " at " << __FILE__ << ":" << __LINE__
                      << std::endl << std::flush;
            exit(1);
        }

        for (int j = 1; j < (int)tokens.size(); j++) {
            off_t chunk_offset = (off_t)Atoi(tokens[j]);
            ocvm_sum_integer_type value_at_point;
            std::string values_at_point_string = "";

            std::map< off_t, std::string > &
                value_mapping = offset_value[filename];

            if (value_mapping.count(chunk_offset) == 0) {
                for (int k = 0; k < 3; k++) {
                    off_t seek_offset;
                    seek_offset = (chunk_offset + k * color_offset);
                    if (fseeko(psumf, seek_offset, SEEK_SET) != 0) {
                        std::cerr << "ERROR: seeking to position "
                                  << chunk_offset << " in file "
                                  << filename
                                  << " at " << __FILE__ << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    if (fread(&value_at_point, sizeof(ocvm_sum_integer_type),
                              1, psumf) < 1) {
                        std::cerr << "ERROR: calling fread"
                                  << " at " << __FILE__
                                  << ":" << __LINE__
                                  << std::endl << std::flush;
                        exit(1);
                    }
                    outb->Append(value_at_point);		
                    values_at_point_string += tostr(value_at_point) + " ";
                }
                value_mapping[chunk_offset] = values_at_point_string;
            }
            else {
                std::vector< std::string > values_at_point = str_tokenize(value_mapping[chunk_offset]);
                for (int k = 0; k < 3; k++) {
                    value_at_point = (ocvm_sum_integer_type)Atoi(values_at_point[k]);
                    outb->Append(value_at_point);
                }
            }
        }
	
        fclose(psumf);
    }

    write(outb, "0");
    return 0;
}
