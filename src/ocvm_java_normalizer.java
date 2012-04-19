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

public class ocvm_java_normalizer extends DCFilter {

    public int process () {
        int i, j;
        String hostname = get_param("myhostname");
        String label = get_param("label");
        System.out.println("Normalizer: my label is " + label);
        int numHosts = Integer.parseInt(get_param("numHosts"));
        int numNormalizers = Integer.parseInt(get_param("numNormalizers"));
        int normalizers_per_host = Integer.parseInt(get_param("normalizers_per_host"));
        double chunks_in_plane = Double.parseDouble(get_param("chunks_in_plane"));
        String channels_to_normalize = get_param("channels_to_normalize");

        int COLS = 512;
        int ROWS = 480;
        int channels = channels_to_normalize.length();
        long[][] backgroundImage = new long[channels][ROWS*COLS];
        int [][] offsetImage = new int[channels][ROWS*COLS];
        int[][] sbackgroundImage = new int[channels][ROWS*COLS];
        int byte_array_size = ROWS * COLS;
        int numChunks = 0;

        for (i = 0; i < channels; i++) {
            for (j = 0; j < ROWS*COLS; j++) {
                backgroundImage[i][j] = 0;
                sbackgroundImage[i][j] = 0;
                offsetImage[i][j] = 255;
            }
        }

        while (true) {
            DCBuffer in = this.read("0");
            int width, height;

            width = in.ExtractInt();
            if (width == -1) {
                break;
            }
            height = in.ExtractInt();

            byte[] array = in.ExtractByteArray(width * height * channels);

            for (j = 0; j < channels; j++) {
                for (int k = 0; k < byte_array_size; k++) {
                    int unsignedValue = array[j*byte_array_size + k] & 0xff;
                    backgroundImage[j][k] = 
                               backgroundImage[j][k] + (long)unsignedValue;
                    if (unsignedValue < offsetImage[j][k]) {
                        offsetImage[j][k] = unsignedValue;
                    }
                }
            }
            numChunks++;
        }

/*
       for (int chan = 0; chan < channels; chan++) {
            try {
            FileOutputStream bFile = new FileOutputStream("/tmp/prebchan" + chan);
            FileOutputStream oFile = new FileOutputStream("/tmp/preochan" + chan);
            DataOutputStream outbw = new DataOutputStream(bFile);
            DataOutputStream outbw1 = new DataOutputStream(oFile);
            for (int k = 0; k < ROWS * COLS; k++) {
                outbw.writeLong(backgroundImage[chan][k]);
                outbw1.writeInt(offsetImage[chan][k]);
            }
            outbw.close();
            outbw1.close();
            }
            catch (Exception e) { System.out.println("E R R O R");}
        }
*/

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

        for (int chan = 0; chan < channels; chan++) {
            for (int k = 0; k < ROWS * COLS; k++) {
                sbackgroundImage[chan][k] = (int)((double)backgroundImage[chan][k] / chunks_in_plane);
            }
        }

/*
       for (int chan = 0; chan < channels; chan++) {
            try {
            FileOutputStream bFile = new FileOutputStream("/tmp/bchan" + chan);
            FileOutputStream oFile = new FileOutputStream("/tmp/ochan" + chan);
            DataOutputStream outbw = new DataOutputStream(bFile);
            DataOutputStream outbw1 = new DataOutputStream(oFile);
            for (int k = 0; k < ROWS * COLS; k++) {
                outbw.writeInt(sbackgroundImage[chan][k]);
                outbw1.writeInt(offsetImage[chan][k]);
            }
            outbw.close();
            outbw1.close();
            }
            catch (Exception e) { System.out.println("E R R O R");}
        }
*/

        MENormalize_ norm = new MENormalize_();
        RenderedImage[] bImg = new RenderedImage[channels];
        RenderedImage[] oImg = new RenderedImage[channels];
        RenderedOp[] meanOp = new RenderedOp[channels];

        for (int chan = 0; chan < channels; chan++) {
            bImg[chan] = norm.preparePAImages(norm.getGSBufferedImage(sbackgroundImage[chan], COLS, ROWS));
            oImg[chan] = norm.preparePAImages(norm.getGSBufferedImage(offsetImage[chan], COLS, ROWS));

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

        int[][] src = new int[channels][ROWS*COLS];
        
        DCBuffer outbuf = new DCBuffer();
        outbuf.AppendInt(1);
        this.write(outbuf, "0");

        while (true) {
            DCBuffer in = this.read("0");
            int width, height;

            width = in.ExtractInt();
            if (width == -1)
                break;
            height = in.ExtractInt();

            byte[] array = in.ExtractByteArray(width * height * channels);
            
            for (int chan = 0; chan < channels; chan++) {
                for (int k = 0; k < width*height; k++) {
                    src[chan][k] = array[width*height*chan + k] & 0xff;
                }

                int[] pixels = norm.normalizeImageInt(norm.getGSBufferedImage(src[chan], COLS, ROWS), 
                                                        oImg[chan], bImg[chan], 
                                                        ((double[])(meanOp[chan].getProperty("mean")))[0]);
                for (i = 0; i < pixels.length; i++) {
                    if (chan == 0) array[i] = int_to_byte(pixels[i]);
                    if (chan == 1) array[width*height + i] = int_to_byte(pixels[i]);
                    if (chan == 2) array[2*width*height + i] = int_to_byte(pixels[i]);
                }
            }

            DCBuffer outb = new DCBuffer();
            outb.AppendByteArray(array);
            this.write(outb, "0");
        }

        src = new int[channels][ROWS*COLS];

        outbuf = new DCBuffer();
        outbuf.AppendInt(1);
        this.write(outbuf, "0");

        while (true) {
            DCBuffer in = this.read("0");
            int width, height;

            width = in.ExtractInt();
            if (width == -1)
                break;
            height = in.ExtractInt();

            byte[] array = in.ExtractByteArray(width * height * channels);

            for (int chan = 0; chan < channels; chan++) {
                for (int k = 0; k < width*height; k++) {
                    src[chan][k] = array[width*height*chan + k] & 0xff;
                }

                int[] pixels = norm.normalizeImageInt(norm.getGSBufferedImage(src[chan], COLS, ROWS),
                                                        oImg[chan], bImg[chan],
                                                        ((double[])(meanOp[chan].getProperty("mean")))[0]);
                for (i = 0; i < pixels.length; i++) {
                    if (chan == 0) array[i] = int_to_byte(pixels[i]);
                    if (chan == 1) array[width*height + i] = int_to_byte(pixels[i]);
                    if (chan == 2) array[2*width*height + i] = int_to_byte(pixels[i]);
                }
            }

            DCBuffer outb = new DCBuffer();
            outb.AppendByteArray(array);
            this.write(outb, "0");
        }
        System.out.println("java normalizer filter: exiting on " + hostname);
        return 0;
    }

    public byte int_to_byte(int x) {
        if (x <= 127) {
            return (byte)x;
        }
        else {
             return (byte)(x-256);
        }
    }
}
