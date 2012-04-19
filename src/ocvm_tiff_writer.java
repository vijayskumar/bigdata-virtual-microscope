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
import java.text.*;
import java.lang.reflect.*;
import jpl.mipl.jade.*;
import ncmir_plugins.imagej_mosaic.*;

public class ocvm_tiff_writer extends DCFilter {
    public int process () {
        String input_filename;
        String tmp_filename;
        String output_filename;
        String output_dir;
        int nx, ny, nz, nc;
        int seenz, seenc;
        int width, height;
        int data_buffers;
        int channel;
        DCBuffer in;
        String mode;
        while (true) {
            // read a filename containing a B or G or R Z-slice same, and
            // write it to disk
            in = read_until_upstream_exit("0");
            if (in == null) {
                break;
            }
            nz = in.ExtractInt();
            nc = in.ExtractInt();
            assert(nc == 3);
            input_filename = in.ExtractString();
            output_filename = in.ExtractString();
            long w = in.ExtractLong();
            width = (int)w;
            long h = in.ExtractLong();
            height = (int)h;
            seenz = seenc = 0;
            for (seenz = 0; seenz < nz; seenz++) {
                for (seenc = 0; seenc < 3; seenc++) {
                    in = read("0");
                    tmp_filename = in.ExtractString();
                    System.out.println("tiff writer allocating " +
                                       width*height + " bytes");
                    byte[] array = new byte[width*height];
                    try {
                        FileInputStream f = new FileInputStream(tmp_filename);
                        f.read(array);
                        f.close();
                        new File(tmp_filename).delete();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                    ByteProcessor newimageproc =
                        new ByteProcessor(width, height);
                    newimageproc.setPixels(array);
                    ImagePlus newimage =
                        new ImagePlus("Combined", newimageproc);
                    FileSaver fs = new FileSaver(newimage);
                    String new_fn = input_filename + ".tmpdir/chan" +
                        (3 - seenc) + "/z" + seenz + ".tif";
                    fs.saveAsTiff(new_fn);
                    newimage.flush();
                }
            }

            DecimalFormat df = new DecimalFormat("0000");
            for (seenz = 0; seenz < nz; seenz++) {
                File files[] = new File[3];
                ImagePlus images[] = new ImagePlus[3];

                for (seenc = 0; seenc < 3; seenc++) {
                    String fn = input_filename + ".tmpdir/chan" +
                        (seenc+1) + "/z" + seenz + ".tif";
                    files[seenc] = new File(fn);
                    images[seenc] = new ImagePlus(files[seenc].getAbsolutePath());
                    new File(fn).delete();
                    String dir = input_filename + ".tmpdir/chan" + (seenc+1);
                    new File(dir).delete();
                }
                String dir = input_filename + ".tmpdir";
                new File(dir).delete();
                
                int indDot = output_filename.lastIndexOf('.');
                String noext = output_filename.substring(0, indDot);
                String ext = output_filename.substring(indDot+1);
                String fn = noext + "_z" + df.format(seenz) + "." + ext;
                File FSave = new File(fn);

                byte[] redPixels, greenPixels, bluePixels;
                redPixels =   (byte[])images[0].getProcessor().getPixels();
                greenPixels = (byte[])images[1].getProcessor().getPixels();
                bluePixels =  (byte[])images[2].getProcessor().getPixels();
					
                RGBTiffEncoderMem saveTiff = new RGBTiffEncoderMem(width,height);
                saveTiff.setRGB(redPixels,greenPixels,bluePixels);
                System.out.println("Saving: " + FSave.getAbsolutePath());
                saveTiff.write(FSave.getAbsolutePath());
            }
        }
        return 0;
    }
}
