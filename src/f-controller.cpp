#include "f-headers.h"

using namespace std;

int ocvm_controller::init()
{
    ocvm_preprocessing_base::init();
    DCBuffer b = this->get_param_buffer("read_orders");
    host_mappings.deSerialize(&b);
    num_tessellations = Atoi(get_param("num_tessellations"));
    return 0;
}

int ocvm_controller::process()
{
    ImageDescriptor * target;
    target = &original_image_descriptor;
        
    std::set<ImagePart> sent_parts;
    for (i = 0; i < tessellations_active_per_node; i++) {
        bool didone = false;
        SerialMap<SerialString, SerialVector<TessReverseMapping> >::iterator it;
        for (it = host_mappings.begin();
             it != host_mappings.end();
             it++) {
            std::string host = it->first;
            std::vector<TessReverseMapping> & vec = it->second;
            if (vec.empty() == false) {
                didone = true;
                TessReverseMapping & trm = vec[0];
                for (u = 0; u < trm.image_coordinates_needed.size(); u++) {
                    ImagePart part = target->get_part(
                        trm.image_coordinates_needed[u]);
                    if (sent_parts.count(part) == 0) {
                        sent_parts.insert(part);
                        std::string & hn = part.hostname;
                        ImageCoordinate & c = part.coordinate;
                        DCBuffer * b = new DCBuffer(256);
                        b->pack("iii", c.x, c.y, c.z);
                        b->Append(hn);
                        b->pack("iii",
                                trm.tess_coordinate.x,
                                trm.tess_coordinate.y,
                                trm.tess_coordinate.z);
                        cout << "controller:  sending out IC " << c;
                        cout << " for TC " << trm.tess_coordinate;
                        cout << " to host " << hn << endl;
                        write_nocopy(b, "0", hn);
                    }
                }
                vec.erase(vec.begin());
            }
        }
        if (!didone) {
            break;
        }
    }

    // from now on, wait for acknowledgments before sending out the next
    // image descriptor
    DCBuffer* in_buffer;
    for (i = 0; i < num_tessellations; i++) {
        in_buffer = read("tessdone");
        assert(in_buffer);
        std::string host;
        in_buffer->Extract(&host);
        // look for next one for this tess host
        SerialVector<TessReverseMapping> & vec =
            host_mappings[host];
        if (vec.empty() == false) {
            TessReverseMapping & trm = vec[0];
            for (u = 0; u < trm.image_coordinates_needed.size(); u++) {
                ImagePart part = target->get_part(
                    trm.image_coordinates_needed[u]);
                if (sent_parts.count(part) == 0) {
                    sent_parts.insert(part);
                    std::string & hn = part.hostname;
                    ImageCoordinate & c = part.coordinate;
                    DCBuffer * b = new DCBuffer;
                    b->pack("iii", c.x, c.y, c.z);
                    b->Append(hn);
                    b->pack("iii",
                            trm.tess_coordinate.x,
                            trm.tess_coordinate.y,
                            trm.tess_coordinate.z);
                    cout << "controller:  sending out IC " << c;
                    cout << " for TC " << trm.tess_coordinate;
                    cout << " to host " << hn << endl;
                    write_nocopy(b, "0", hn);
                }
            }
            vec.erase(vec.begin());
        }
    }
    return 0;
}
