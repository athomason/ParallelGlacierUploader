import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadResult;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadResult;
import com.amazonaws.services.glacier.model.UploadMultipartPartRequest;
import com.amazonaws.services.glacier.model.UploadMultipartPartResult;
import com.amazonaws.util.BinaryUtils;

// Based on example code from http://docs.amazonwebservices.com/amazonglacier/latest/dev/uploading-an-archive-mpu-using-java.html

public class ParallelGlacierUploader {
	
    public static String vaultName = "Backup";
    public static int maxThreads = 16;
    public static int timeoutDays = 1000;
  
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
        	throw new IOException("Filename required");
        }
        (new ParallelGlacierUploader(args[0])).upload();
    }
    
    private String archiveFilePath;
    private int partSize;
    private String partSizeStr;
    private long fileSize;
    private int numParts;
    private AmazonGlacierClient client;
    private List<byte[]> binaryChecksums;
	private String uploadId;
	private String checksum;
	private int numPartsDone;
    
	public ParallelGlacierUploader(String filename) throws IOException {
		archiveFilePath = filename;
		
    	InputStream prop = ParallelGlacierUploader.class.getResourceAsStream("AwsCredentials.properties");
        AWSCredentials credentials = new PropertiesCredentials(prop);
        client = new AmazonGlacierClient(credentials);
        client.setEndpoint("https://glacier.us-east-1.amazonaws.com/");
        
        File file = new File(archiveFilePath);
        FileInputStream fileToUpload = new FileInputStream(file);
        fileSize = file.length();
        fileToUpload.close();
        
        // Part size must be a power of two and no less than 1MB. The number of parts may not exceed 10,000.
        // This computes the smallest part size yielding fewer than 10k parts.
        partSize = Math.max(1024 * 1024, (int) Math.pow(2, Math.ceil(Math.log(fileSize / 10000) / Math.log(2))));
        partSizeStr = String.format("%d", partSize);
        numParts = (int) Math.ceil(fileSize / partSize);

        System.out.println(String.format(
    		"Uploading %,dMB in %,d blocks of %,dMB.",
    		(long) fileSize / (1024 * 1024),
    		numParts,
    		partSize / (1024 * 1024)));
        
        numPartsDone = 0;
        
        binaryChecksums = Collections.synchronizedList(new ArrayList<byte[]>(numParts));
        for (int i = 0; i < numParts; i++)
        	binaryChecksums.add(null);
	}
		
	public void upload() throws Exception {
        initiateMultipartUpload();
        uploadParts();
        completeMultiPartUpload();
    }
    
    private void initiateMultipartUpload() {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest()
        .withVaultName(vaultName)
        .withArchiveDescription("Archive " + (new Date()))
        .withPartSize(partSizeStr);            
        
        InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
        
        uploadId = result.getUploadId();
        System.out.println("ArchiveID: " + uploadId);
    }

    private void uploadParts() throws Exception {
    	final ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
    	
    	final Date start = new Date();
    	for (int part = 0; part < numParts; part++) {
    		final int p = part;
    		executor.execute(new Thread(new Runnable() {
    	    	public void run() {
    	    		try {
    	    			uploadPart(p);
    	    			Date end = new Date();
    	    			long secs = (end.getTime() - start.getTime()) / 1000;
    	    	    	long remainingSecs = (long) (numParts - numPartsDone) * (secs / numPartsDone);
    	    	    	System.out.println(String.format("Upload %.1f%% complete (%d/%d), approximate finish in %,.1fhr (%s)",
    	    	    			100 * (float) numPartsDone / numParts, numPartsDone, numParts,
    	    	    			(float) remainingSecs / 3600,
    	    	    			new Date(end.getTime() + 1000 * remainingSecs)));
    	    		}
    	    		catch (Exception e) {
    	    			System.out.println("Yikes! " + e);
    	    			executor.shutdownNow();
    	    		}
    	    	}
    		}, "Part #" + part));
    	}
    	executor.shutdown();
    	try {
    		executor.awaitTermination(timeoutDays, TimeUnit.DAYS);
    	}
		catch (InterruptedException e) {
			System.err.println("Interrupted while waiting for threads: " + e);
		}
    	
    	Date end = new Date();
    	long elapsed = (end.getTime() - start.getTime()) / 1000;
    	System.out.println(String.format(
    			"Finished uploading %d parts at " + end + " in %,ds (%,.3fMB/s)",
    			numPartsDone,
    			elapsed,
    			(float) numPartsDone * partSize / elapsed));
    	
    	if (numParts == numPartsDone) {
        	checksum = TreeHashGenerator.calculateTreeHash(binaryChecksums);
    	}
    	else {
        	throw new Exception("Completed the wrong number of parts: " + numParts);
    	}
    }
    
    private void uploadPart(int partNum) throws IOException {
    	long position = (long) partSize * partNum;
    	byte[] buffer = new byte[partSize];

    	File file = new File(archiveFilePath);
    	RandomAccessFile fileToUpload = new RandomAccessFile(file, "r");
    	fileToUpload.seek(position);
    	int read = fileToUpload.read(buffer, 0, buffer.length);
        fileToUpload.close();
        
    	if (read == -1) { return; }
    	byte[] bytesRead = Arrays.copyOf(buffer, read);
    	buffer = null;

    	String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(bytesRead));
    	byte[] binaryChecksum = BinaryUtils.fromHex(checksum);
    	binaryChecksums.set(partNum, binaryChecksum);
    	
    	String contentRange = String.format("bytes %s-%s/*", position, position + read - 1);

    	UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
    	.withVaultName(vaultName)
    	.withBody(new ByteArrayInputStream(bytesRead))
    	.withChecksum(checksum)
    	.withRange(contentRange)
    	.withUploadId(uploadId);               

    	Date start = new Date();
    	System.out.println("Starting on part #" + partNum + " @ " + start);
    	UploadMultipartPartResult partResult = client.uploadMultipartPart(partRequest);
    	Date end = new Date();
    	
    	System.out.println(end);
    	System.out.println(end + ": checksum #" + partNum + ": " + partResult.getChecksum());
    	long seconds = (end.getTime() - start.getTime()) / 1000;
    	float rate = read / (1024 * seconds);
    	System.out.println(String.format("Transferred %,d bytes in %ds @ %.1fKB/s",
    			read, seconds, rate));
    	
    	numPartsDone++;
    }

    private void completeMultiPartUpload() throws NoSuchAlgorithmException, IOException {
        
        File file = new File(archiveFilePath);

        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
            .withVaultName(vaultName)
            .withUploadId(uploadId)
            .withChecksum(checksum)
            .withArchiveSize(String.valueOf(file.length()));
        
        CompleteMultipartUploadResult compResult = client.completeMultipartUpload(compRequest);
        
        System.out.println("Completed an archive. ArchiveId: " + compResult.getLocation());
    }
}