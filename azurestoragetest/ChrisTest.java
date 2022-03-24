package net.redpoint.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.storage.common.ParallelTransferOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;

public class ChrisTest {
	// Fill in these values
	static final String ADL2_ACCOUNT_NAME = "redpointtest2";
	static final String ADL2_ACCESS_KEY = "";
	static final String ADL2_CONTAINER_NAME = "redpointtest2";

	static final int THREADS = 24;
	static final long FILE_SIZE = 100 * 1024 * 1024L;
	static final int TIMEOUT_SECONDS = 6000;

	static final long FILE_UPLOAD_BLOCK_SIZE = 1024 * 1024 * 100;

	static final int FILE_UPLOAD_PARALLELISM = 2;
	static final String AZURE_STORAGE_HOST_SUFFIX = ".dfs.core.windows.net/";
	static final int adl2_max_tries = 2;
	static final int adl2_try_timeout_in_seconds = 10000; /// arbitrarily large, change to something small and can
															/// reproduce
	static final long adl2_retry_delay_in_ms = 60;
	static final long adl2_max_retry_delay_in_ms = 60000;

	public static void main(String[] args) throws Exception {
		System.setProperty("AZURE_LOG_LEVEL", "true");

		Random r = new Random();
		File temp = File.createTempFile("adl2test", ".tmp");
		try (OutputStream os = new FileOutputStream(temp)) {
			byte[] chunk = new byte[1024];
			for (long l = 0; l < FILE_SIZE; l += chunk.length) {
				r.nextBytes(chunk);
				os.write(chunk);
			}
		}
		DataLakeServiceClientBuilder serviceClientBuilder = new DataLakeServiceClientBuilder()
				.endpoint("https://" + ADL2_ACCOUNT_NAME + AZURE_STORAGE_HOST_SUFFIX)
				.httpLogOptions(new HttpLogOptions().setLogLevel(HttpLogDetailLevel.HEADERS)
						.addAllowedHeaderName("x-ms-client-request-id") // Change. Along with a properties file, a
																		// dependency, and AZURE_LOG_LEVEL=VERBOSE
						.addAllowedHeaderName("x-ms-request-id"))
				.retryOptions(new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, adl2_max_tries // Maximum number of
																									// attempts an
																									// operation will be
																									// retried, default
																									// is 4
						, adl2_try_timeout_in_seconds // Maximum time allowed before a request is cancelled and assumed
														// failed, default is Integer.MAX_VALUE
						, adl2_retry_delay_in_ms // Amount of delay to use before retrying an operation, default value
													// is 4ms when retryPolicyType is EXPONENTIAL
						, adl2_max_retry_delay_in_ms // Maximum delay allowed before retrying an operation, default
														// value is 120ms
						, null // secondaryHost - Secondary Storage account to retry requests against, default
								// is none
				));
		serviceClientBuilder.credential(new StorageSharedKeyCredential(ADL2_ACCOUNT_NAME, ADL2_ACCESS_KEY));
		DataLakeServiceClient serviceClient = serviceClientBuilder.buildClient();
		DataLakeFileSystemClient fileSystemClient = serviceClient.getFileSystemClient(ADL2_CONTAINER_NAME);
		ExecutorService exec = Executors.newFixedThreadPool(THREADS);
		List<Future<Void>> futures = new ArrayList<>();
		for (int i = 0; i < THREADS; i++) {
			futures.add(exec.submit(() -> {
				String cloudPath = UUID.randomUUID().toString();
				DataLakeFileClient fileClient = fileSystemClient.getFileClient(cloudPath);
				DataLakeRequestConditions requestConditions = new DataLakeRequestConditions();
				Duration timeout = Duration.ofSeconds(TIMEOUT_SECONDS);
				ParallelTransferOptions options = new ParallelTransferOptions();
				options.setBlockSizeLong(FILE_UPLOAD_BLOCK_SIZE);
				options.setMaxConcurrency(FILE_UPLOAD_PARALLELISM);
				options.setMaxSingleUploadSizeLong(1L); // forcing to upload in blocks for demonstration/logging
				options.setProgressReceiver(bytes -> // Change
				System.out.println("File " + cloudPath + "; bytes: " + bytes + "; time: " + OffsetDateTime.now()));
				System.out.println("Uploading " + temp + " to " + cloudPath);
				fileClient.uploadFromFile(temp.toString(), options, null, null, requestConditions, timeout);
				System.out.println("Finished uploading " + temp + " to " + cloudPath);
				return null;
			}));
		}
		DataLakeFileClient dldfc;
		for (Future<Void> future : futures) {
			future.get();
		}
		System.out.println("Uploads complete");
		System.exit(0);
	}

}
