import java.io.*;
import java.util.GregorianCalendar;
import java.util.Calendar;

public class ocvmjimgreader {
	public static final int sizeInt = 4;
	public static final int sizeFloat = 4;
	public static final int sizeByte = 1;
	public static final int sizeLong = 4;
	public static final int sizeShort = 2;
	public static final int sizeChar = 1;
	public static final int sizeDouble = 8;

	public static final int MAINHEADER_SIZE = 44;
	public static final String SIGNATURE = "NIKON IMAGE FORMATED FILE";

	//private DataInputStream DISinput;
	private RandomAccessFile file;
	public int[] psVersion;	
	public MontParam[] montageInfo;
	public TimeParam   timeInfo;
    public PMTInfo[] pmts;
    public int[][] laserPowers;

	public int nTotSeqNum;
	public long nNextSeqOffset;
	public long nCurSeqOffset;
	public int nCurSeqNumber;
	public long nCurSeqFooterOffset;
	public long nCurImageOffset;
	public int nCurBitDepth;
	public int nCurNumChn;
	public int nCurNumImages;
	public float fPixResolution;
	public int nCurNumMontages;
	public int nCurHeight;
	public int nCurWidth;
    public int nAverage;
    public float fAvgLaserPowers[];
    public float fMinMaxLaserPowers[][]; 
    public String szObjLens;
    public String szPinhole;
    public int nPinholePos;
    public ExcitationFilterInfo ExFilters[];
    public FilterCubeInfo       FilterCubes[];
    public int nBeamExpander;
    public GregorianCalendar TimeDateExperiment;
    public float fOpticalZoom;
    public String szBand;

	public ocvmjimgreader(String szFileName){
		//DataInputStream DISinput = new DataInputStream( new
		//	BufferedInputStream( new FileInputStream(szDirectory +
		//	szFileName)));
		try {
			file = new RandomAccessFile(szFileName, "r");
		}
		catch (IOException e){
			showError("Unable to open file: " + e.getMessage());
		}
		psVersion = new int[2];
	}
    public void showError(String ms)
    {
        System.err.println(ms);
    }
	public boolean getMainHeader(){
		char[] cBuf = new char[64];
		try {
			for (int i = 0; i < 32; i++){
				cBuf[i] = (char)file.read();
			}
			String szSig = new String(cBuf);
			
			if (!szSig.startsWith(SIGNATURE)){
				showError("Error: Header Signature does not match that of"
                          + " a *.img file");
				return false;
			}

			psVersion[0] = readInvertShort();
			psVersion[1] = readInvertShort();
			
			nTotSeqNum = readInvertInt();

			nNextSeqOffset = (0x00000000FFFFFFFF) & readInvertInt();
			nCurSeqOffset = nNextSeqOffset;
            nCurSeqNumber = 1;
		}
		catch (IOException e){
			showError("Error, getMainHeader():" + e.getMessage());
			return false;
		}
		return true;
	}
	
	public boolean getSequenceHeader(){
		char[] cBuf = new char[64];

		try {
			file.seek(nNextSeqOffset);
			//Skipping ulSeqNum
			file.skipBytes(4);
			
            if (nCurSeqNumber > nTotSeqNum) return false; 
            else  nCurSeqNumber++; 

			if (psVersion[0] >= 1 && psVersion[1] >=2){
				nCurImageOffset = readInvertInt64() + nNextSeqOffset;
				nCurSeqFooterOffset = readInvertInt64() + nNextSeqOffset;

				nCurSeqOffset = nNextSeqOffset;
				nNextSeqOffset = (0x00000000FFFFFFFF) & readInvertInt64() + nCurSeqOffset;
			}
			else {
				//Read in ulImageAddr
				nCurImageOffset = (0xFFFFFFFFL) & (readInvertInt()) + nNextSeqOffset; 

				//ulFooterAddr, ulNextSeqAddr
				nCurSeqFooterOffset = ((0xFFFFFFFFL) & (readInvertInt())) + nNextSeqOffset;
				nCurSeqOffset = nNextSeqOffset;
				nNextSeqOffset = (0xFFFFFFFFL) & (readInvertInt()) + nCurSeqOffset;
			}
			//Reading in sSchNum
			nCurNumChn = readInvertShort();

			//Reading lNumOfImage
			nCurNumImages = readInvertInt() * nCurNumChn;

			//Skipping lAlignX and lAlignY of ImageArea
			file.skipBytes(8);
			nCurWidth =  readInvertInt();
			nCurHeight = readInvertInt();
			nCurBitDepth = readInvertInt();
			file.skipBytes(sizeLong);
			fPixResolution = readInvertFloat();
		}
		catch (IOException e){
			showError("Error, getSequenceHeader():" + e.getMessage());
			return false;
		}
		
		return true;
	}

	public boolean getSequenceFooter(){
		try {
			if (nCurSeqFooterOffset < nCurSeqOffset){
				showError("Error: footer offset address is invalid." +
                          " Most likely due to a file greater than 4 gigabytes");
				return false;
			}
			file.seek(nCurSeqFooterOffset);

			// Skipping Time structure for now

            TimeDateExperiment = new GregorianCalendar();
            TimeDateExperiment.set(Calendar.YEAR, readInvertShort());
            TimeDateExperiment.set(Calendar.MONTH, readInvertShort());
            TimeDateExperiment.set(Calendar.DATE, readInvertShort());
            TimeDateExperiment.set(Calendar.HOUR, readInvertShort());
            TimeDateExperiment.set(Calendar.MINUTE, readInvertShort());
            TimeDateExperiment.set(Calendar.SECOND, readInvertShort());
			int hund = readInvertShort();
			int totaltime = readInvertLong(); 

			//file.skipBytes(sizeLong);
			file.skipBytes(512 * sizeChar);
			file.skipBytes(32 * sizeChar);
			file.skipBytes(32 * sizeChar);
			file.skipBytes(32 * sizeChar);
			file.skipBytes(512 * sizeChar);
			
			// objlensinfo
            szObjLens = readString(64);

			//file.skipBytes(64 * sizeChar);
			file.skipBytes(sizeFloat);
			file.skipBytes(sizeFloat);
			file.skipBytes(16 * sizeChar);
			
			// Skipping ChannelInfo
			for (int i =0; i < nCurNumChn; i++){
				file.skipBytes(sizeShort);
                nAverage = readInvertInt();
				//file.skipBytes(sizeLong);
				file.skipBytes(sizeLong);
				file.skipBytes(sizeShort);
				file.skipBytes(512 * sizeChar);
				file.skipBytes(sizeShort);
				file.skipBytes(512 * sizeChar);
			}

			int sNumOfArea = readInvertShort();
			// Skipping RcmAreaInfo
			if (sNumOfArea < 0){
				return false;
			}
			for (int i = 0; i < sNumOfArea; i++){
				file.skipBytes(sizeShort);
				file.skipBytes(sizeShort);
				file.skipBytes(sizeShort);

				// union structure here
				file.skipBytes(sizeShort * 4);
				file.skipBytes(sizeShort * 4);
			}
			
			// pucDispLut
			file.skipBytes(nCurNumChn * sizeChar * 256);
			
			// RcmPseudoLut tPseudoLut
			file.skipBytes(sizeShort);
			file.skipBytes(sizeShort);
			file.skipBytes(sizeDouble * 34 * 3);
			file.skipBytes(sizeDouble * 6 * 8);

			// long * plPixelClockDelay
			file.skipBytes(sizeLong * nCurNumChn);
				
			// RcmScanInfo tScan
			fOpticalZoom = readInvertFloat();
			file.skipBytes(sizeShort);
			file.skipBytes(sizeShort);

            int band = readInvertShort();
			file.skipBytes(sizeLong);

            szBand = new String(band +" (0:full, 1:1/2, 2:1/4, 4:1/8)");
			
			// RcmPinholeInfo tPinhole
            szPinhole = readString(64);
            szPinhole = szPinhole.trim();
            int nPinholePos = readInvertShort();
			
			// sExFilterNum, RcmExFilterInfo * ptExFilter
			int filternum = readInvertShort();
			if (filternum < 0){
				return false;
			}
            ExFilters = new ExcitationFilterInfo[filternum];
			for (int i = 0; i < filternum; i++){
                /*
                  file.skipBytes(sizeShort);
                  file.skipBytes(64 * sizeChar);
                  file.skipBytes(64 * sizeChar);
                  file.skipBytes(sizeShort);
                  file.skipBytes(sizeShort);
                */
                ExFilters[i] = new ExcitationFilterInfo();
                ExFilters[i].nNumber = readInvertShort();
                ExFilters[i].szLaserName = readString(64);
                ExFilters[i].szFilterName = readString(64);
                ExFilters[i].nPosition = readInvertShort();
                ExFilters[i].nVisible = readInvertShort();
			}
			
			// lExpander
			//file.skipBytes(sizeLong);
            nBeamExpander = readInvertInt();

			// sFilterCubeNu, RcmFilterCubeInfo * ptFilterCube
			int filternumcubes = readInvertShort();
			if (filternumcubes < 0){
				return false;
			}
            FilterCubes = new FilterCubeInfo[filternumcubes];
			for (int i = 0;  i < filternumcubes; i++){
                /*
                  file.skipBytes(sizeShort);
                  file.skipBytes(sizeChar * 64);
                  file.skipBytes(sizeShort);
                */
                FilterCubes[i] = new FilterCubeInfo();
                FilterCubes[i].nNumber = readInvertShort();
                FilterCubes[i].szFilterName = readString(64);
                FilterCubes[i].nPosition = readInvertShort();
			}
			
			// RcmZControlInfo tzCtrl
			file.skipBytes(sizeShort);
			file.skipBytes(sizeLong);
			file.skipBytes(sizeLong);
			file.skipBytes(sizeLong);
			file.skipBytes(sizeLong);
			file.skipBytes(sizeLong);
			
			// Skipping	ptFrameTimes
			for (int i = 0; i < (nCurNumImages/nCurNumChn); i++){
				file.skipBytes(sizeLong);
				file.skipBytes(sizeLong);
			}
			
			// ptPmt
            int gainSum[] = {0,0,0,0};
            int offsetSum[] = {0,0,0,0};

            pmts = new PMTInfo[nCurNumChn];
			for (int i = 0; i < (nCurNumImages/nCurNumChn); i++){
				for (int j = 0; j < nCurNumChn; j++){
					file.skipBytes(sizeShort);
                    gainSum[j] += readInvertLong();
                    offsetSum[j] += readInvertLong();
				}
			}
            for (int i = 0; i < nCurNumChn; i++){
                pmts[i] = new PMTInfo();
                pmts[i].nGain = gainSum[i]/(nCurNumImages/nCurNumChn);
                pmts[i].nOffset = offsetSum[i]/(nCurNumImages/nCurNumChn);
            }
            

            // Number of lasers
			int nNumLaser = readInvertShort();
			if (nNumLaser < 0){
				return false;
			}

			//plLaserPower
            int nLaserSum = 0;;
            fAvgLaserPowers = new float[nNumLaser];
            fMinMaxLaserPowers = new float[nNumLaser][2];
            laserPowers = new int[nNumLaser][nCurNumImages/nCurNumChn];
			for (int i = 0; i < nNumLaser; i++){
                // Initialie values for minmax
                fMinMaxLaserPowers[i][0] = 20000;
                fMinMaxLaserPowers[i][1] = Float.MIN_VALUE;
				for (int j = 0; j < (nCurNumImages/nCurNumChn); j++){
                    laserPowers[i][j] = readInvertLong();
                    nLaserSum +=  laserPowers[i][j];
                    if (laserPowers[i][j] < fMinMaxLaserPowers[i][0]){
                        fMinMaxLaserPowers[i][0] = laserPowers[i][j];
                    }
                    if (laserPowers[i][j] > fMinMaxLaserPowers[i][1])
                        fMinMaxLaserPowers[i][1] = laserPowers[i][j];
				}
                fAvgLaserPowers[i] = (float)nLaserSum/(nCurNumImages/nCurNumChn);
			}

			int nNumOfMark = readInvertInt();	

			// Skipping ptMark
			if (nNumOfMark < 0){
				return false;
			}
			for (int i = 0; i < nNumOfMark; i++){
				file.skipBytes(sizeChar * 64);	
				file.skipBytes(sizeLong);
				file.skipBytes(sizeLong);
			}

			if (psVersion[0] >= 1 && psVersion[1] >= 0){
				nCurNumMontages = readInvertInt();
				if (nCurNumMontages < 0){
					return false;
				}
				timeInfo = new TimeParam();

				// Get Montage related Information
				if (nCurNumMontages == 0) {
					montageInfo = new MontParam[1];

					montageInfo[0] = new MontParam();
					montageInfo[0].nImgX = readInvertInt();
					montageInfo[0].nImgY = readInvertInt();
					montageInfo[0].nImgZ = readInvertInt();
                    if (psVersion[0] >= 1 && psVersion[1] >= 3)
                        montageInfo[0].nDistortionCutoff = readInvertInt();

					montageInfo[0].dOverlap = readInvertDouble();
					montageInfo[0].dzDist = readInvertDouble();
				}
				else {
					montageInfo = new MontParam[nCurNumMontages];
					for (int i = 0; i < nCurNumMontages;i++){
						montageInfo[i] = new MontParam();
						montageInfo[i].nImgX = readInvertInt();
						montageInfo[i].nImgY = readInvertInt();
						montageInfo[i].nImgZ = readInvertInt();
                        if (psVersion[0] >= 1 && psVersion[1] >= 3)
                            montageInfo[i].nDistortionCutoff = readInvertInt();

						montageInfo[i].dOverlap = readInvertDouble();
						montageInfo[i].dzDist = readInvertDouble();

					}
				}
				
				timeInfo.nTimeDelay = readInvertInt();
				timeInfo.nTimeDuration = readInvertInt();
				
			}
		}
		catch (IOException e){
			showError("Error, getSequenceFooter()" + e.getMessage());
			return false;
		}
		return true;
	}

    /* public String getMetadata()
     *   
     * returns the metadata from the last sequence processed and returns it as a String.
     * if no sequences were processed, then this will be blank.
     */
    public String getMetadata(){
        String notes = "";

        // Scanning info.
        notes = notes.concat("Objective Lens: " + this.szObjLens + "\n");
        notes = notes.concat("Optical Zoom: " + Float.toString(this.fOpticalZoom) + "\n"); 
        notes = notes.concat("Band: " + this.szBand + "\n");
        notes = notes.concat("Beam Expander Number: " + this.nBeamExpander + "\n");
        notes = notes.concat("Pinhole: " + this.szPinhole + "\n");
        notes = notes.concat("Images Averaged: " + this.nAverage);
        notes = notes.concat("\n");

        // Montage Info.
        for (int i = 0 ; i < this.nCurNumMontages;i++){
            notes = notes.concat( "Montage :" + (i+1)  + "\n");
            notes = notes.concat( "\tNumber of Images in X: " + this.montageInfo[i].nImgX  + "\n");
            notes = notes.concat( "\tNumber of Images in Y: " + this.montageInfo[i].nImgY  + "\n");
            notes = notes.concat( "\tNumber of Images in Z: " + this.montageInfo[i].nImgZ  + "\n");
            notes = notes.concat( "\tOverlap percentage: " + this.montageInfo[i].dOverlap  + "\n");
            notes = notes.concat( "\tZ distance: " + this.montageInfo[i].dzDist + "\n");

            // Distortion Cutoff Info
            if (this.psVersion[0] >= 1 && this.psVersion[1] >= 3){
                notes = notes.concat("Distortion cutoff: " + this.montageInfo[i].nDistortionCutoff + "\n") ;
            }
            if (i == this.nCurNumMontages - 1)
                notes = notes.concat("\n");
        }


        // Time (4D) Params 
        if (this.nCurNumMontages > 0){
            notes = notes.concat("Time Delay: " + this.timeInfo.nTimeDelay + "\n");
            notes = notes.concat("Time Duration: " + this.timeInfo.nTimeDuration + "\n");
            notes = notes.concat("\n");
        }

        // Laser Info
        for (int i = 0; i < this.fAvgLaserPowers.length; i++){
            notes = notes.concat("Average Laser (" + (i+1) + ") Power : " 
                                 + Float.toString(this.fAvgLaserPowers[i]) + "\n"); 
            notes = notes.concat("Min Laser (" + (i+1) + ") Power : "
                                 + fMinMaxLaserPowers[i][0] + "\n");
            notes = notes.concat("Max Laser (" + (i+1) + ") Power : " 
                                 + fMinMaxLaserPowers[i][1] + "\n");
            if (i == this.fAvgLaserPowers.length - 1)
                notes = notes.concat("\n");
        }

        // Resolution
        notes = notes.concat("Resolution (Pixels/micron): " + this.fPixResolution + "um\\px" + "\n");
        notes = notes.concat("\n");
    
        // Filters
        for (int i = 0; i < this.ExFilters.length; i++){
            notes = notes.concat("Filter (" + this.ExFilters[i].nNumber + "):" + "\n");
            notes = notes.concat("\t laser name: " + this.ExFilters[i].szLaserName + "\n");
            notes = notes.concat("\t filter name: " + this.ExFilters[i].szFilterName + "\n"); 
            notes = notes.concat("\t filter position: " + this.ExFilters[i].nPosition + "\n");
            //notes.concat("\t Visible or Infrared: " + this.ExFilters[i].nVisible);
            if (i == this.ExFilters.length - 1)
                notes = notes.concat("\n");
        }
    
        // Filter Cube Info
        for (int i = 0; i < this.FilterCubes.length; i++){
            notes = notes.concat("Filter Cube (" + this.FilterCubes[i].nNumber + ")" + "\n");
            notes = notes.concat("\t filter cube name: " +  this.FilterCubes[i].szFilterName  + "\n"); 
            notes = notes.concat("\t filter cube position : " + this.FilterCubes[i].nPosition  + "\n");
            if (i == this.FilterCubes.length - 1)
                notes = notes.concat("\n");
        }
    
        // Pmt info
        for (int i = 0; i < this.nCurNumChn; i++){
            notes = notes.concat("PMT (" + i + ")" + "\n");
            notes = notes.concat("\t Gain: " + this.pmts[i].nGain + "\n");
            notes = notes.concat("\t Offset: " + this.pmts[i].nOffset + "\n");
            if (i == this.nCurNumChn)
                notes = notes.concat("\n");
        }
        return notes;
    }
	private int readByte() throws IOException{
		return file.read();
	}

	private double readInvertDouble() throws IOException{
		long temp1 = 0;
		long temp2 = 0;
		long temp3 = 0;
		long temp4 = 0;
		long temp5 = 0;
		long temp6 = 0;
		long temp7 = 0;
		long temp8 = 0;

		temp1 = readByte();
		temp2 = readByte();
		temp3 = readByte();
		temp4 = readByte();
		temp5 = readByte();
		temp6 = readByte();
		temp7 = readByte();
		temp8 = readByte();

		return Double.longBitsToDouble(
			(temp8 << 56) | (temp7 << 48)| (temp6 << 40) | (temp5 << 32) | 
			(temp4 << 24) | (temp3 << 16) | (temp2 << 8) |(temp1 & 0xffffffff)
                                       );
	}
	private float readInvertFloat() throws IOException{
		int temp1 = 0;
		int temp2 = 0;	
		int temp3 = 0;	
		int temp4 = 0;	

		temp1 = readByte();
		temp2 = readByte();
		temp3 = readByte();
		temp4 = readByte();

		return Float.intBitsToFloat((temp4 << 24) | (temp3 << 16) | (temp2 << 8)
                                    | (temp1 & 0xffffffff));
	}

	private int readInvertLong() throws IOException{
		return readInvertInt();
	}

	private int readInvertInt() throws IOException{
		int temp1 = 0;	
		int temp2 = 0;	
		int temp3 = 0;	
		int temp4 = 0;	

		temp1 = readByte();
		temp2 = readByte();
		temp3 = readByte();
		temp4 = readByte();

		return ((temp4 << 24) | (temp3 << 16) | (temp2 << 8)
                | (temp1 & 0xffffffff));
    }

	private long readInvertInt64() throws IOException{
		long temp1 = 0;	
		long temp2 = 0;	
		long temp3 = 0;	
		long temp4 = 0;	
		long temp5 = 0;	
		long temp6 = 0;	
		long temp7 = 0;	
		long temp8 = 0;	

		temp1 = readByte();
		temp2 = readByte();
		temp3 = readByte();
		temp4 = readByte();
		temp5 = readByte();
		temp6 = readByte();
		temp7 = readByte();
		temp8 = readByte();
	
		return ((temp8 << 56) | (temp7 << 48)| (temp6 << 40) | (temp5 << 32) | 
                (temp4 << 24) | (temp3 << 16) | (temp2 << 8) | (temp1 &
                                                                0xffffffff));
    }
    private String readString(int nBytes) throws IOException{
        char[] cBuf = new char[nBytes];
        boolean bEndFlag = false;
        int i;
        for (i = 0; i < nBytes; i++){
            cBuf[i] = (char)file.read();
            if (cBuf[i] == 0){
                break;
            }
        }
        file.skipBytes(nBytes - i-1);
        String retString = new String(cBuf);
        return retString;
    }

    private int readInvertShort() throws IOException{
		int temp1 = 0;
		int temp2 = 0;

		temp1 = readByte();
		temp2 = readByte();

		return ((temp2 << 8) | (temp1 & 0xffff));
    }

    class MontParam{
		public int nImgX;	
		public int nImgY;	
		public int nImgZ;	
        public int nDistortionCutoff;

		public double dOverlap;
		public double dzDist;
    }

    class TimeParam{
		public int nTimeDelay;
		public int nTimeDuration;
    }
    
    class ExcitationFilterInfo{
        public String szFilterName;
        public String szLaserName;
        public int    nNumber;
        public int    nPosition;
        public int    nFilterNumber;
        public int    nVisible;
    }
    class FilterCubeInfo{
        public int    nNumber;
        public String szFilterName;
        public int    nPosition;
    }
    class PMTInfo {
        public int nOffset;
        public int nGain;           
    }
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: ocvmjimgreader <file.IMG>");
            System.exit(1);
        }
        String fn = args[0];
        if (!new File(fn).exists()) {
            System.err.println("ERROR: " + fn + " doesn't exist");
            System.exit(1);
        }
        long sz = new File(fn).length();
        ocvmjimgreader r = new ocvmjimgreader(fn);
        r.getMainHeader();
        StringBuffer output= new StringBuffer("");
        output.append("Version: " +
                      Integer.toString(r.psVersion[0]) + "." +
                      Integer.toString(r.psVersion[1])+"\n");
        output.append("NSequences: " + r.nTotSeqNum);
        for (int l = 0; l < r.nTotSeqNum; l++){
            output.append("\n");
            r.getSequenceHeader();
            r.getSequenceFooter();
            output.append("Sequence: " + l + "\n");
//             output.append(r.getMetadata());
            int nImgX;	
            int nImgY;	
            int nImgZ;
            nImgX = r.montageInfo[l].nImgX;
            nImgY = r.montageInfo[l].nImgY;
            nImgZ = r.montageInfo[l].nImgZ;
            output.append("NImgX: " + nImgX + "\n");
            output.append("NImgY: " + nImgY + "\n");
            output.append("NImgZ: " + nImgZ + "\n");

            output.append("nDistortionCutoff: " + r.montageInfo[l].nDistortionCutoff + "\n");
            output.append("dOverlap: " + r.montageInfo[l].dOverlap + "\n");
            output.append("dzDist: " + r.montageInfo[l].dzDist + "\n");
            
            output.append("width: " + r.nCurWidth + "\n");
            output.append("height: " + r.nCurHeight + "\n");
            output.append("offset: " + r.nCurImageOffset + "\n");
            long occupied = (long)r.nCurImageOffset +
                ((long)r.nCurHeight*(long)r.nCurWidth*(long)nImgX*(long)nImgY*(long)nImgZ*(long)r.nCurNumChn);
            if (occupied > sz) {
                System.err.println("ERROR: metadata suggests file should be at least " + occupied + " bytes, but it's only " + sz + " bytes!");
                System.exit(1);
            }
        }
        System.out.print(output);
    }
}
