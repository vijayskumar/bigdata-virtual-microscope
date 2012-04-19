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
import java.lang.reflect.*;
import jpl.mipl.jade.*;
import ncmir_plugins.imagej_mosaic.*;

class AutoAlignmentResult {
    public AutoAlignmentResult(
        int score, int x_displacement, int y_displacement,
        boolean left_to_right, int diffX, int diffY)
    {
        this.score = score;
        this.x_displacement = x_displacement;
        this.y_displacement = y_displacement;
        this.left_to_right = left_to_right;
        this.diffX = diffX;
        this.diffY = diffY;
    }
    public int score;
    public int x_displacement;
    public int y_displacement;
    public boolean left_to_right;
    public int diffX;
    public int diffY;
    public String toString()
    {
        return "score=" + score +
            ", x_displacement=" + x_displacement +
            ", y_displacement=" + y_displacement +
            ", left_to_right=" + left_to_right +
            ", diffX=" + diffX +
            ", diffY=" + diffY;
    }
}

public class ocvm_java_aligner extends DCFilter {
    
    public static AutoAlignmentResult align_byte_arrays(
        byte[] a1,
        byte[] a2,
        double overlap_percentage,
        int width,
        int height,
        boolean left_to_right)
    {
        Autoalign aligner = new Autoalign(width, height);
        ByteProcessor bp1 = new ByteProcessor(width, height);
        ByteProcessor bp2 = new ByteProcessor(width, height);

        int diffX;
        int diffY;
        if (left_to_right) {
            diffX = (int)(width * (1-overlap_percentage));
            if ((diffX % 2) != 0) {
                diffX--;
            }
            diffY = 0;
            diffX = 0 - diffX; // vijay knows why
            aligner.calcRectX(diffX, diffY);
        }
        else {
            diffX = 0;
            diffY = (int)(height * (1-overlap_percentage));
            if ((diffY % 2) != 0) {
                diffY--;
            }
            aligner.calcRectY(diffX, diffY);
        }
//         System.out.println("diffX, diffY " + diffX + "," + diffY);

        byte[] new_a1 = aligner.getCommonAreaSingle(a1, (double)diffX, (double)diffY, width, height);
        bp1.setPixels(new_a1);
        bp2.setPixels(a2);

        Point tr = aligner.getTranslation(bp1.createImage(),
                                          bp2.createImage());
        AutoAlignmentResult out = new AutoAlignmentResult(
            aligner.getLastScore(),
            tr.x, tr.y,
            left_to_right,
            diffX, diffY);
        return out;
    }
    
    public int process () {
        int i;
        String hostname = get_bind_host();
        int images_merged = 0;
        
        while (true) {
            DCBuffer in = this.read_until_upstream_exit("0");
            if (in == null) {
                break;
            }
            images_merged++;
            int width, height;
            int byteArraySize;
            double overlap;
            boolean left_to_right;
            int ref_x, ref_y, subj_x, subj_y;

            left_to_right = (in.ExtractInt() == 1) ? true : false;
            width = in.ExtractInt();
            height = in.ExtractInt();
            overlap = in.ExtractDouble();
            if (overlap <= 0) {
                System.err.println("ERROR: overlap is " + overlap);
                System.exit(1);
            }
            ref_x = in.ExtractInt();
            ref_y = in.ExtractInt();
            subj_x = in.ExtractInt();
            subj_y = in.ExtractInt();
            byte[] reference = in.ExtractByteArray(width*height);
            byte[] subject = in.ExtractByteArray(width*height);
            long before = System.currentTimeMillis();
            AutoAlignmentResult out = ocvm_java_aligner.align_byte_arrays(
                reference,
                subject,
                overlap / 100.0,
                width, height, left_to_right);
            long after = System.currentTimeMillis();
//             System.out.println("this iteration took " + (after - before)
//                                + " milliseconds on host " +
//                                hostname);
            DCBuffer outbuf = new DCBuffer();
            outbuf.AppendInt(out.score);
            outbuf.AppendInt(out.x_displacement);
            outbuf.AppendInt(out.y_displacement);
            outbuf.AppendInt(out.left_to_right?1:0);
            outbuf.AppendInt(out.diffX);
            outbuf.AppendInt(out.diffY);
            this.write(outbuf, "0");
        }
        System.out.println("java aligner filter: exiting on " + hostname);
        return 0;
    }
}
