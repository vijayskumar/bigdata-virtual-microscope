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

public class ocvm_java_prenormalizer extends DCFilter {

    public int process () {
        int i, j;
        String hostname = get_param("myhostname");
        String label = get_param("label");
        System.out.println("Prenormalizer: my label is " + label);
        int numHosts = Integer.parseInt(get_param("numHosts"));
        int numNormalizers = Integer.parseInt(get_param("numNormalizers"));
        int normalizers_per_host = Integer.parseInt(get_param("normalizers_per_host"));
        String channels_to_normalize = get_param("channels_to_normalize");

        int COLS = Integer.parseInt(get_param("tileWidth"));
        int ROWS = Integer.parseInt(get_param("tileHeight"));
        int channels = Integer.parseInt(get_param("nchannels"));
	int chunksizeX = Integer.parseInt(get_param("chunksizeX"));
	int chunksizeY = Integer.parseInt(get_param("chunksizeY"));
	int nXChunks = Integer.parseInt(get_param("nXChunks"));
	int nYChunks = Integer.parseInt(get_param("nYChunks"));

        long[][] backgroundImage = new long[channels][ROWS*COLS];
        int [][] offsetImage = new int[channels][ROWS*COLS];
        int[][] sbackgroundImage = new int[channels][ROWS*COLS];
        int byte_array_size = ROWS * COLS;
	int chunksize = chunksizeX * chunksizeY * COLS * ROWS * channels;

        for (i = 0; i < channels; i++) {
            for (j = 0; j < ROWS*COLS; j++) {
                backgroundImage[i][j] = 0;
                sbackgroundImage[i][j] = 0;
                offsetImage[i][j] = 255;
            }
        }

        while (true) {
            DCBuffer in = this.read("0");
            int ischunk;

            ischunk = in.ExtractInt();
            if (ischunk == 0) {
                break;
            }

            byte[] array = in.ExtractByteArray(chunksize);

	    for (i = 0; i < chunksizeX*chunksizeY; i++) {
                for (j = 0; j < channels; j++) {
                    for (int k = 0; k < byte_array_size; k++) {
                        int unsignedValue = array[i*byte_array_size*channels + j*byte_array_size + k] & 0xff;
                        backgroundImage[j][k] = 
                                   backgroundImage[j][k] + (long)unsignedValue;
                        if (unsignedValue < offsetImage[j][k]) {
                            offsetImage[j][k] = unsignedValue;
                        }
                    }
                }
	    }
	    in = null;
        }

        int rows_per_normalizer = ROWS / numNormalizers;
        int rows_per_normalizer_last = rows_per_normalizer;
        int rows_per_host = ROWS / numHosts;
        StringTokenizer toks = new StringTokenizer(label, "_");
        toks.nextToken(); 
        i = Integer.parseInt(toks.nextToken()); 
        j = Integer.parseInt(toks.nextToken()); 
        if (i == numHosts-1 && j == normalizers_per_host-1) rows_per_normalizer += ROWS % numNormalizers;
        rows_per_normalizer_last += ROWS % numNormalizers;

        long[][] my_backgroundImage = new long[channels][COLS * rows_per_normalizer];
        int[][] my_offsetImage = new int[channels][COLS * rows_per_normalizer];

        for (int iter = 0; iter < 2; iter++) {
            int my_start_offset = (i * rows_per_host + (j + iter) * ROWS/numNormalizers) % ROWS;
            if (iter == 1 && i == numHosts-1 && j == normalizers_per_host-1) my_start_offset = 0;
            int my_offset = my_start_offset * COLS;

            for (int count = 0; count < numNormalizers-1; count++) {
                for (int chan = 0; chan < channels; chan++) {
                    for (int k = 0; k < COLS * rows_per_normalizer; k++) {
                        my_backgroundImage[chan][k] = backgroundImage[chan][my_offset + k];
                        my_offsetImage[chan][k] = offsetImage[chan][my_offset + k];
                    }
                }
                int src_offset = my_offset - (ROWS/numNormalizers) * COLS;
                if (src_offset < 0) src_offset = (ROWS - rows_per_normalizer_last) * COLS; 
                DCBuffer out = new DCBuffer((8*COLS + 4*COLS) * rows_per_normalizer * channels);
                for (int chan = 0; chan < channels; chan++) {
                    for (int k = 0; k < COLS * rows_per_normalizer; k++) {
                        out.AppendLong(my_backgroundImage[chan][k]);
                    }
                    for (int k = 0; k < COLS * rows_per_normalizer; k++) {
                        out.AppendInt(my_offsetImage[chan][k]);
                    }
                }
                write(out, "to_higher");

                DCBuffer newpacket = read("from_lower");
                int rows_to_read = ROWS / numNormalizers;
                if (i == 0 && j == 0) rows_to_read = rows_per_normalizer_last;
                for (int chan = 0; chan < channels; chan++) {
                    for (int k = 0; k < COLS * rows_to_read; k++) {
                        long templ = newpacket.ExtractLong();
                        if (iter == 0)
                            backgroundImage[chan][src_offset + k] += templ;
                        else
                            backgroundImage[chan][src_offset + k] = templ;
                    }
                    for (int k = 0; k < COLS * rows_to_read; k++) {
                        int temp2 = newpacket.ExtractInt();
                        if (iter == 0) {
                            if (offsetImage[chan][src_offset + k] > temp2) offsetImage[chan][src_offset + k] = temp2;
                        }
                        else 
                            offsetImage[chan][src_offset + k] = temp2;
                    }
                }
                my_offset = my_offset - (ROWS / numNormalizers) * COLS;
                if (my_offset < 0) my_offset = (ROWS - rows_per_normalizer_last) * COLS;
            }
        }

	DCBuffer outb = new DCBuffer();
	DCBuffer outb2 = new DCBuffer();
	double chunks_in_plane = nXChunks * nYChunks;
        for (int chan = 0; chan < channels; chan++) {
            for (int k = 0; k < ROWS * COLS; k++) {
                sbackgroundImage[chan][k] = (int)((double)backgroundImage[chan][k] / chunks_in_plane);
	 	outb.AppendInt(sbackgroundImage[chan][k]);	
		outb2.AppendInt(offsetImage[chan][k]);
            }
        }
	if (label.equals("N_0_0_java")) { 	
	    this.write(outb, "to_console");
	    this.write(outb2, "to_console");
	}

        System.out.println("java prenormalizer filter: exiting on " + hostname);
        return 0;
    }
}
