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

// for SHA1
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class AlignmentType {
    public static final int NORMAL      = 0;
    public static final int EXTRA_RIGHT = 1;
    public static final int EXTRA_LEFT  = 2;
    public static final int EXTRA_DOWN  = 3;
    public static final int EXTRA_UP    = 4;
}

class AlignmentResult {
    public AlignmentResult(
        int score, int x_displacement, int y_displacement,
        int x_ref, int y_ref, int x_subject, int y_subject,
        boolean left_to_right, int diffX, int diffY)
    {
        this.score = score;
        this.x_displacement = x_displacement;
        this.y_displacement = y_displacement;
        this.x_ref = x_ref;
        this.y_ref = y_ref;
        this.x_subject = x_subject;
        this.y_subject = y_subject;
        this.left_to_right = left_to_right;
        this.diffX = diffX;
        this.diffY = diffY;
    }
    public int score;
    public int x_displacement;
    public int y_displacement;
    public int x_ref;
    public int y_ref;
    public int x_subject;
    public int y_subject;
    public boolean left_to_right;
    public int diffX;
    public int diffY;
    public String toString()
    {
        return "score=" + score +
            ", x_displacement=" + x_displacement +
            ", y_displacement=" + y_displacement +
            ", x_ref=" + x_ref +
            ", y_ref=" + y_ref +
            ", x_subject=" + x_subject +
            ", y_subject=" + y_subject +
            ", left_to_right=" + left_to_right +
            ", diffX=" + diffX +
            ", diffY=" + diffY;
    }
}


class AlignmentExecutor {
    private int width;
    private int height;
    private int rows;
    private int columns;
    private int x_offset;
    private int y_offset;
    private int byteArraySize;
    private double overlap;
    private byte[][][] grid;
    private int arrays_received;
    private PrintWriter monitor_writer;
    
    public AlignmentExecutor(int width, int height,
                             int rows, int columns, int x_offset, int y_offset,
                             int byteArraySize, double overlap,
                             PrintWriter monitor_writer)
    {
        this.width = width;
        this.height = height;
        this.rows= rows;
        this.columns= columns;
        this.x_offset= x_offset;
        this.y_offset= y_offset;
        this.byteArraySize = byteArraySize;
        this.overlap = overlap;
        this.monitor_writer = monitor_writer;
        grid = new byte[rows][columns][];
        arrays_received= 0;

    }
    // note: something below here is not thread safe
    public static AlignmentResult align_byte_arrays(byte[] a1,
                                                    byte[] a2,
                                                    double overlap_percentage,
                                                    int width,
                                                    int height,
                                                    int x_ref,
                                                    int y_ref,
                                                    int x_subject,
                                                    int y_subject,
                                                    boolean left_to_right,
                                                    PrintWriter monitor_writer)
    {
//         System.out.println("align_byte_arrays: (" + x_ref + "," + y_ref +
//                            ") <-> (" + x_subject + "," + y_subject + ")");

        // compute SHA1
//         try {
//             MessageDigest md = MessageDigest.getInstance( "SHA" );
//             md.update(a1);
//             byte[] digest = md.digest();
//             System.out.print("SHA1 (Java): for x,y of " + x_ref + "," + y_ref
//                              + ": ");
//             for (int i2 = 0; i2 < digest.length; i2++) {
//                 System.out.printf("%02x", digest[i2]);
//             }
//             System.out.println();
//         }
//         catch (Exception e) {
//             e.printStackTrace();
//         }
//         try {
//             MessageDigest md = MessageDigest.getInstance( "SHA" );
//             md.update(a2);
//             byte[] digest = md.digest();
//             System.out.print("SHA1 (Java): for x,y of " + x_subject + "," +
//                              y_subject + ": ");
//             for (int i2 = 0; i2 < digest.length; i2++) {
//                 System.out.printf("%02x", digest[i2]);
//             }
//             System.out.println();
//         }
//         catch (Exception e) {
//             e.printStackTrace();
//         }

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
        AlignmentResult out = new AlignmentResult(aligner.getLastScore(),
                                                  tr.x, tr.y,
                                                  x_ref, y_ref,
                                                  x_subject, y_subject,
                                                  left_to_right,
                                                  diffX, diffY);
        //System.out.println("aligned byte arrays, result is " + out);

        if (monitor_writer != null) {
            String hostname;
            try {
                InetAddress addr = InetAddress.getLocalHost();
                hostname = addr.getHostName();
            }
            catch (UnknownHostException e) {
                hostname = "UNKNOWN";
            }
            String message = "edgein " + hostname + " " +
                x_ref + " " + y_ref + " " +
                ((left_to_right)?"1":"0") + "\n";
            try {
                monitor_writer.print(message);
                monitor_writer.flush();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        return out;
    }
    public void purge_subimage_from_cache(int x, int y) {
//         System.out.println("purge_subimage_from_cache("
//                            + x+","+ y+")");
        int x_storageloc = x - x_offset;
        int y_storageloc = y - y_offset;
        assert( grid[y_storageloc][x_storageloc] != null);
        grid[y_storageloc][x_storageloc] = null;
    }
    public Vector<AlignmentResult> feed (
        byte[] array, int x, int y, int type) {
//         System.out.println("feed("+x+","+y+"), type=" + type);
        Vector<AlignmentResult> outval = new Vector<AlignmentResult>(3);
        AlignmentResult result;
        int x_storageloc = x - x_offset;
        int y_storageloc = y - y_offset;
        switch (type) {
            case AlignmentType.NORMAL:
                grid[y_storageloc][x_storageloc] = array;
                // process left to right alignments
                if (x > x_offset) { // not in first column
                    result =
                        align_byte_arrays(
                            grid[y_storageloc][x_storageloc-1],
                            grid[y_storageloc][x_storageloc],
                            overlap/100.0, width, height, x-1, y, x, y, true,
                            monitor_writer);
                    outval.add(result);
                }
                // process top to bottom alignments
                if (y > y_offset) {
                    result =
                        align_byte_arrays(
                            grid[y_storageloc-1][x_storageloc],
                            grid[y_storageloc][x_storageloc],
                            overlap/100.0, width, height, x, y-1, x, y, false,
                            monitor_writer);
                    outval.add(result);
                }
                break;
            case AlignmentType.EXTRA_RIGHT:
                result =
                    align_byte_arrays(
                        grid[y_storageloc][x_storageloc-1],
                        array,
                        overlap/100.0, width, height, x-1, y, x, y, true,
                        monitor_writer);
                outval.add(result);
                break;
            case AlignmentType.EXTRA_LEFT:
                result =
                    align_byte_arrays(
                        array,
                        grid[y_storageloc][0],
                        overlap/100.0, width, height, x, y, x+1, y, true,
                        monitor_writer);
                outval.add(result);
                break;
            case AlignmentType.EXTRA_DOWN:
                result =
                    align_byte_arrays(
                        grid[y_storageloc-1][x_storageloc],
                        array,
                        overlap/100.0, width, height, x, y-1, x, y, false,
                        monitor_writer);
                outval.add(result);
                break;
            case AlignmentType.EXTRA_UP:
                result =
                    align_byte_arrays(
                        array,
                        grid[0][x_storageloc],
                        overlap/100.0, width, height, x, y, x, y+1, false,
                        monitor_writer);
                outval.add(result);
                break;
        }

        return outval;
    }
}

public class ocvm_local_aligner extends DCFilter {
    
    public void deliver_results(Vector<AlignmentResult> results) {
        for (int i = 0; i < results.size(); i++) {
            AlignmentResult result = results.elementAt(i);
            DCBuffer out = new DCBuffer(10 * 4);
            out.AppendInt(result.score);
            out.AppendInt(result.x_displacement);
            out.AppendInt(result.y_displacement);
            out.AppendInt(result.x_ref);
            out.AppendInt(result.y_ref);
            out.AppendInt(result.x_subject);
            out.AppendInt(result.y_subject);
            out.AppendInt(result.left_to_right?1:0);
            out.AppendInt(result.diffX);
            out.AppendInt(result.diffY);
            write(out, "to_mst");
        }
    }

    public void process_subregion_default(
        String label,
        AlignmentExecutor executor,
        int width,
        int height,
        int rows,
        int columns,
        int global_rows,
        int global_columns,
        int x_offset,
        int y_offset) {
        int i;
        int row, column;
        int global_row, global_column;
        int byte_array_size = width * height;
        DCBuffer request_buffer = new DCBuffer();
        request_buffer.AppendString(label);
        int requests = 0;
        for (row = 0; row < rows; row++) {
            global_row = row + y_offset;
            for (column = 0; column < columns; column++) {
                global_column = column + x_offset;
//                 System.out.println("looking at column " +
//                                    column + ", row " + row);
//                 System.out.println("global_column " + global_column
//                                    + ", global_row " + global_row);
                requests++;
                boolean request_extra_left_subimage = false;
                boolean request_extra_right_subimage = false;
                boolean request_extra_up_subimage = false;
                boolean request_extra_down_subimage = false;
                if ((global_row + global_column)%2==0) {
                    if (column==columns-1 &&
                        global_column+1 < global_columns) {
                        request_extra_right_subimage = true;
                        requests++;
                    }
                    if (column == 0 && global_column > 0) {
                        request_extra_left_subimage = true;
                        requests++;
                    }
                    if (row==rows-1 && global_row+1 < global_rows) {
                        request_extra_down_subimage = true;
                        requests++;
                    }
                    if (row==0 && global_row > 0) {
                        request_extra_up_subimage = true;
                        requests++;
                    }
                }

                request_buffer.AppendInt(AlignmentType.NORMAL);
                request_buffer.AppendInt(global_column);
                request_buffer.AppendInt(global_row);
                if (request_extra_up_subimage) {
                    request_buffer.AppendInt(AlignmentType.EXTRA_UP);
                    request_buffer.AppendInt(global_column);
                    request_buffer.AppendInt(global_row-1);
                }
                if (request_extra_left_subimage) {
                    request_buffer.AppendInt(AlignmentType.EXTRA_LEFT);
                    request_buffer.AppendInt(global_column-1);
                    request_buffer.AppendInt(global_row);
                }
                if (request_extra_right_subimage) {
                    request_buffer.AppendInt(AlignmentType.EXTRA_RIGHT);
                    request_buffer.AppendInt(global_column+1);
                    request_buffer.AppendInt(global_row);
                }
                if (request_extra_down_subimage) {
                    request_buffer.AppendInt(AlignmentType.EXTRA_DOWN);
                    request_buffer.AppendInt(global_column);
                    request_buffer.AppendInt(global_row+1);
                }
            }
        }
        this.write(request_buffer, "subimage_request");
            
        for (i = 0; i < requests; i++) {
            DCBuffer newpacket = read("subimage_data");
            int type = newpacket.ExtractInt();
            int x = newpacket.ExtractInt();
            int y = newpacket.ExtractInt();
            byte[] array = newpacket.ExtractByteArray(byte_array_size);
            deliver_results(executor.feed(array, x, y, type));
            if (y > y_offset && type == AlignmentType.NORMAL) {
                executor.purge_subimage_from_cache(x, y - 1);
            }
        }
    }
    
    public int process () {
        int i;
        String hostname = get_param("my_hostname");
        String label = get_param("label");
        System.out.println("Aligner: my label is " + label);
        boolean wait_for_display_setter= has_param("wait_for_display_setter");
        if (wait_for_display_setter) {
            DCBuffer input = read("from_display_setter");
        }
        
        boolean ocvmstitchmonon;
        Socket ocvmstitchmon_socket= null;
        PrintWriter monitor_writer = null;

        ocvmstitchmonon = (System.getenv("OCVMSTITCHMON") != null);
        if (ocvmstitchmonon) {
            if (ocvmstitchmon_socket==null) {
                String var = System.getenv("OCVMSTITCHMON");
                String monhostname = var.split(":")[0];
                int port = Integer.parseInt(var.split(":")[1]);
                try {
                    ocvmstitchmon_socket = new Socket(monhostname, port);
                    monitor_writer =
                        new PrintWriter(ocvmstitchmon_socket.getOutputStream());
                }
                catch (Exception e) {
                    System.err.println("ERROR:  could not connect to " + var);
                    e.printStackTrace();
                }
            }
        }

        int regions_asked = 0;
        while (true) {
            // ask for another region
            DCBuffer out = new DCBuffer();
            out.AppendString(label);
            out.AppendString(hostname);
            this.write(out, "subregion_request");
            regions_asked++;
                
            System.out.println(label + " ready for another subregion");
            DCBuffer in = read("subregion_request");
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

            AlignmentExecutor executor = new AlignmentExecutor(
                width, height, rows, columns, x_offset, y_offset,
                width * height, overlap, monitor_writer);
            
            if (true) { // simplest access pattern, add more later
                process_subregion_default(label, executor, width, height,
                                          rows, columns,
                                          global_rows, global_columns,
                                          x_offset, y_offset);
            }
        }

        if (ocvmstitchmonon) {
            try {
                if (monitor_writer != null && ocvmstitchmon_socket != null) {   
                    monitor_writer.close();
                    ocvmstitchmon_socket.close();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }

        }

        System.out.println("aligner filter: exiting on " + hostname);
        return 0;
    }
}
