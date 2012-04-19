import java.io.*;
import java.net.*;
import dcmpi.*;
import ij.*;
import ij.process.*;
import ij.io.*;
import java.math.*;
import javax.media.jai.*;
import javax.swing.*;
import java.awt.image.*;
import java.awt.image.renderable.*;
import java.awt.*;
import java.awt.color.*;
import java.util.*;
import java.lang.*;
import java.lang.reflect.*;
import jpl.mipl.jade.*;
import ncmir_plugins.jai_mosaic.MENormalize_;
import ncmir_plugins.imagej_mosaic.Autoalign;

public class ocvm_normalizer extends DCFilter {

        public RandomAccessFile tmpStore;
        public int process () {
                int i, j;
                String hostname = get_param("my_hostname");
                String label = get_param("label");
                System.out.println("Normalizer: my label is " + label);
                int numHosts = Integer.parseInt(get_param("numHosts"));
                int numNormalizers = Integer.parseInt(get_param("numNormalizers"));
                int normalizers_per_host = Integer.parseInt(get_param("normalizers_per_host"));
                int numChunks = Integer.parseInt(get_param("numChunks"));
                String scratchdir = get_param("scratchdir");
                String channels_to_normalize = get_param("channels_to_normalize");
        
                int regions_asked = 0;
                int overall_requests = 0;
                int COLS = 512;
                int ROWS = 480;
                int channels = channels_to_normalize.length();
                long[][] backgroundImage = new long[channels][ROWS*COLS];
                byte [][] offsetImage = new byte[channels][ROWS*COLS];
                int[][] sbackgroundImage = new int[channels][ROWS*COLS];
                int[][] soffsetImage = new int[channels][ROWS*COLS];
                try {
                    tmpStore = new RandomAccessFile(scratchdir + "/ocvmtemp", "rw");                
                }
                catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                while (true) {
                        // ask for another region
                        DCBuffer out = new DCBuffer();
                        out.AppendString(label);
                        out.AppendString(hostname);
                        this.write(out, "zproj_subregion_request");
                        regions_asked++;

                        System.out.println(label + " ready for another subregion");
                        DCBuffer in = read("zproj_subregion_request");
                        String more = in.ExtractString();
                        if (more.equals("bye")) {
                                System.out.println(label + ": saying goodbye");
                                break;
                        }
                        int width, height;
                        int rows, columns, x_offset, y_offset, byteArraySize;
                        int global_rows;
                        int global_columns;
                        double overlap;
                        width = in.ExtractInt();
                        height = in.ExtractInt();
                        rows = in.ExtractInt();
                        columns = in.ExtractInt();
                        global_rows = in.ExtractInt();
                        global_columns = in.ExtractInt();
                        x_offset = in.ExtractInt();
                        y_offset = in.ExtractInt();
                        byteArraySize = width * height;
                        overlap = in.ExtractDouble();
//             System.out.println("java width are:    " + width);
//             System.out.println("java height are:    " + height);
//             System.out.println("java rows are:    " + rows);
//             System.out.println("java columns are: " + columns);
//             System.out.println("java global_rows are:    " + global_rows);
//             System.out.println("java global_columns are: " + global_columns);
//             System.out.println("java x_offset is: " + x_offset);
//             System.out.println("java y_offset is: " + y_offset);
//             System.out.println("java overlap is: " + overlap);
                        System.out.println(hostname + ": got region, column_cursor is " + x_offset + ", row_cursor is " + y_offset);

                        if (overlap <= 0) {
                                System.err.println("ERROR: overlap is " + overlap);
                                System.exit(1);
                        }

                        int requests = 0;
                        int row, column;
                        int global_row, global_column;
                        int byte_array_size = width * height;
                        DCBuffer request_buffer = new DCBuffer();
                        request_buffer.AppendString(label);

                        for (row = 0; row < rows; row++) {
                                global_row = row + y_offset;
                                for (column = 0; column < columns; column++) {
                                        global_column = column + x_offset;
                                        requests++;
                                        request_buffer.AppendInt(AlignmentType.NORMAL);
                                        request_buffer.AppendInt(global_column);
                                        request_buffer.AppendInt(global_row);
                                }
                        }
                        this.write(request_buffer, "zproj_subimage_request");

                            //backgroundImage = new int[byte_array_size];
                            //offsetImage = new byte[byte_array_size];
                        if (regions_asked == 1) {
                            for (i = 0; i < channels; i++) {
                                for (j = 0; j < byte_array_size; j++) {
                                    backgroundImage[i][j] = 0;
                                    offsetImage[i][j] = (new Integer(255)).byteValue();
                                }
                            }
                        }
                        for (i = 0; i < requests; i++) {
                            DCBuffer newpacket = read("zproj_subimage_data");
                            int type = newpacket.ExtractInt();
                            int x = newpacket.ExtractInt();
                            int y = newpacket.ExtractInt();
                            byte[] array = newpacket.ExtractByteArray(byte_array_size * channels);
                            try {
                                //tmpStore.writeInt(x); 
                                //tmpStore.writeInt(y); 
                                tmpStore.write(array); 
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                                System.exit(1);
                            }

                            for (j = 0; j < channels; j++) {
                                for (int k = 0; k < byte_array_size; k++) {
                                    backgroundImage[j][k] = backgroundImage[j][k] + ((long)array[j*byte_array_size + k] + 128);                // Naive conversion of byte to long in Java
                                    if (array[j*byte_array_size + k] < offsetImage[j][k]) offsetImage[j][k] = array[j*byte_array_size + k];
                                }
                            }
                        }
                        overall_requests += requests;
                }

                int rows_per_normalizer = ROWS / numNormalizers;
                int rows_per_host = ROWS / numHosts;
                StringTokenizer toks = new StringTokenizer(label, "_");
                toks.nextToken(); 
                i = Integer.parseInt(toks.nextToken()); 
                j = Integer.parseInt(toks.nextToken()); 

                long[][] my_backgroundImage = new long[channels][COLS * rows_per_normalizer];
                byte[][] my_offsetImage = new byte[channels][COLS * rows_per_normalizer];

                for (int iter = 0; iter < 2; iter++) {
                    int my_start_offset = (i * rows_per_host + (j + iter) * rows_per_normalizer) % ROWS;
                    int my_offset = my_start_offset;

                    for (int count = 0; count < numNormalizers-1; count++) {
                        for (int chan = 0; chan < channels; chan++) {
                            for (int k = 0; k < COLS * rows_per_normalizer; k++) {
                                my_backgroundImage[chan][k] = backgroundImage[chan][my_offset + k];
                                my_offsetImage[chan][k] = offsetImage[chan][my_offset + k];
                            }
                        }
                        int src_offset = my_offset - rows_per_normalizer;
                        if (src_offset < 0) src_offset = ROWS + src_offset;
                        DCBuffer out = new DCBuffer((8*COLS + COLS) * rows_per_normalizer * channels);
                        for (int chan = 0; chan < channels; chan++) {
                            for (int k = 0; k < COLS * rows_per_normalizer; k++) {
                                out.AppendLong(my_backgroundImage[chan][k]);
                            }
                            out.AppendByteArray(my_offsetImage[chan]);
                            write(out, "to_higher");
                        }

                        DCBuffer newpacket = read("from_lower");
                        for (int chan = 0; chan < channels; chan++) {
                            for (int k = 0; k < COLS * rows_per_normalizer; k++) {
                                long templ = newpacket.ExtractLong();
                                if (iter == 0)
                                    backgroundImage[chan][src_offset + k] += templ;
                                else
                                    backgroundImage[chan][src_offset + k] = templ;
                            }
                            byte[] array = newpacket.ExtractByteArray(COLS * rows_per_normalizer);
                            for (int k = 0; k < COLS * rows_per_normalizer; k++) {
                                if (iter == 0) {
                                    if (offsetImage[chan][src_offset + k] < array[k]) offsetImage[chan][src_offset + k] = array[k];
                                }
                                else 
                                    offsetImage[chan][src_offset + k] = array[k];
                            }
                        }
                        my_offset = my_offset - rows_per_normalizer;
                        if (my_offset < 0) my_offset = ROWS + my_offset;
                    }
                }

                for (int chan = 0; chan < channels; chan++) {
                    for (int k = 0; k < ROWS * COLS; k++) {
                        backgroundImage[chan][k] /= numChunks;
                        sbackgroundImage[chan][k] = (int)backgroundImage[chan][k];                      // No loss of precision will occur here ..so typecast not a problem.
                        soffsetImage[chan][k] = (int)offsetImage[chan][k];
                    }
                }

                MENormalize_ norm = new MENormalize_();
                RenderedImage[] bImg = new RenderedImage[channels];
                RenderedImage[] oImg = new RenderedImage[channels];
                RenderedOp[] meanOp = new RenderedOp[channels];
                for (int chan = 0; chan < channels; chan++) {
                    bImg[chan] = norm.preparePAImages(norm.getGSBufferedImage(sbackgroundImage[chan], COLS, ROWS));
                    oImg[chan] = norm.preparePAImages(norm.getGSBufferedImage(soffsetImage[chan], COLS, ROWS));

                    ParameterBlockJAI pb;
                    // Compute avg minus min.
                    pb = new ParameterBlockJAI("subtract");
                    pb.addSource(bImg[chan]);
                    pb.addSource(oImg[chan]);
                    bImg[chan] = JAI.create("subtract", pb);

                    // Compute mean of new avg.
                    pb = new ParameterBlockJAI("mean");
                    pb.addSource(bImg[chan]);
                    meanOp[chan] = JAI.create("mean", pb);
                }

                byte[][] src = new byte[channels][ROWS*COLS];
                int[][] isrc = new int[channels][COLS*ROWS];
                for (int iter = 0; iter < overall_requests; iter++) { 
                    for (int chan = 0; chan < channels; chan++) {
                        try {
                            tmpStore.seek((iter+chan)*ROWS*COLS);
                            tmpStore.read(src[chan]);
                            for (int k = 0; k < COLS*ROWS; k++) {
                                isrc[chan][k] = (int)src[chan][k] + 128;
                            }

                            norm.normalizeImage(norm.getGSBufferedImage(isrc[chan], COLS, ROWS), oImg[chan], bImg[chan], ((double[])(meanOp[chan].getProperty("mean")))[0]);

                            for (int k = 0; k < COLS*ROWS; k++) {
                                src[chan][k] = new Integer(isrc[chan][k]).byteValue();
                            }
                            tmpStore.seek((iter+chan)*ROWS*COLS);
                            tmpStore.write(src[chan]);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                            System.exit(1);
                        }
                    }
                }
                try {
                    tmpStore.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                return 0;
        }

}
