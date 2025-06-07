import logging
import os
import subprocess
import sys
import time
from collections import Counter

# Configure logging for better visibility into script execution
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class JxlConverter:
    """
    A class to convert JPEG/JPG files to JXL format and report metrics to Prometheus
    via the Node Exporter's textfile collector.
    """

    def __init__(self, source_directory, metrics_directory, cjxl_path="cjxl"):
        """
        Initializes the JxlConverter.

        Args:
            source_directory (str): The root directory to scan for JPEG/JPG files.
            metrics_directory (str): The directory where Prometheus metric files will be written.
                                     Node Exporter should be configured to read from this path.
            cjxl_path (str): The path to the cjxl executable. Defaults to "cjxl"
                             assuming it's in the system's PATH.
        Raises:
            FileNotFoundError: If the specified source_directory does not exist.
        """
        if not os.path.isdir(source_directory):
            # If the source directory doesn't exist, try to create it and add dummy files for testing
            logging.warning(f"Source directory '{source_directory}' not found.")
            try:
                os.makedirs(source_directory, exist_ok=True)
                self._create_dummy_files(source_directory)
                logging.info(f"Source directory '{source_directory}' created with dummy files.")
            except Exception as e:
                raise FileNotFoundError(
                    f"Source directory not found and could not create it: {source_directory}. Error: {e}")

        self.source_directory = source_directory
        self.metrics_directory = metrics_directory
        self.cjxl_path = cjxl_path

        # Initialize metrics
        self.total_space_saved_bytes = 0  # Cumulative space saved across all runs
        self.successful_conversions = 0  # Cumulative count of successful conversions
        self.failed_conversions = 0  # Cumulative count of failed conversions
        self.failed_reasons = Counter()  # Counter for specific failure reasons
        self.last_interval_space_saved_bytes = 0  # Space saved in the current script run

        # Ensure the metrics directory exists for Node Exporter
        os.makedirs(self.metrics_directory, exist_ok=True)
        logging.info(f"JXL Converter initialized.")
        logging.info(f"Scanning images in: {self.source_directory}")
        logging.info(f"Prometheus metrics will be written to: {self.metrics_directory}")
        logging.info(f"Using cjxl executable at: {self.cjxl_path}")

    def _create_dummy_files(self, directory):
        """
        Creates some dummy JPG files for testing purposes if the directory is empty.
        """
        if not os.listdir(directory):  # Only create if directory is empty
            logging.info(f"Creating dummy JPG files in '{directory}' for testing.")
            try:
                # Create a dummy large JPG file (5 MB random data)
                with open(os.path.join(directory, "test_image_large.jpg"), "wb") as f:
                    f.write(os.urandom(1024 * 1024 * 5))
                # Create a dummy medium JPG file (500 KB random data)
                with open(os.path.join(directory, "test_image_medium.jpeg"), "wb") as f:
                    f.write(os.urandom(1024 * 500))
                # Create a subdirectory and another dummy file
                subdir = os.path.join(directory, "subdirectory")
                os.makedirs(subdir, exist_ok=True)
                with open(os.path.join(subdir, "another_image.jpg"), "wb") as f:
                    f.write(os.urandom(1024 * 700))  # 700 KB random data
                logging.info("Dummy JPG files successfully created.")
            except Exception as e:
                logging.error(f"Failed to create dummy files in '{directory}': {e}")
                logging.error("Please manually add some JPG files to the source directory for testing.")

    def _generate_metrics_file(self):
        """
        Generates a Prometheus-compatible metrics file in the specified metrics directory.
        This file will be read by Node Exporter's textfile collector.
        """
        metrics_content = []

        # Total conversions attempted
        metrics_content.append(f"# HELP jpeg_to_jxl_conversions_total Total JPEG to JXL conversions attempted.")
        metrics_content.append(f"# TYPE jpeg_to_jxl_conversions_total counter")
        metrics_content.append(f"jpeg_to_jxl_conversions_total {self.successful_conversions + self.failed_conversions}")

        # Total successful conversions
        metrics_content.append(
            f"# HELP jpeg_to_jxl_conversions_successful_total Total successful JPEG to JXL conversions.")
        metrics_content.append(f"# TYPE jpeg_to_jxl_conversions_successful_total counter")
        metrics_content.append(f"jpeg_to_jxl_conversions_successful_total {self.successful_conversions}")

        # Total failed conversions, labeled by reason
        metrics_content.append(f"# HELP jpeg_to_jxl_conversions_failed_total Total failed JPEG to JXL conversions.")
        metrics_content.append(f"# TYPE jpeg_to_jxl_conversions_failed_total counter")
        if not self.failed_reasons:
            # Ensure the metric exists even if no failures occurred
            metrics_content.append(f'jpeg_to_jxl_conversions_failed_total 0')
        else:
            for reason, count in self.failed_reasons.items():
                # Prometheus labels should be alphanumeric and underscores.
                # Standardize common error reasons for cleaner labels.
                standardized_reason = reason.replace(" ", "_").replace("-", "_").lower()
                metrics_content.append(
                    f'jpeg_to_jxl_conversions_failed_total{{reason="{standardized_reason}"}} {count}')

        # Cumulative total space saved
        metrics_content.append(
            f"# HELP jpeg_to_jxl_space_saved_bytes_total Total space saved by JXL conversions in bytes.")
        metrics_content.append(f"# TYPE jpeg_to_jxl_space_saved_bytes_total gauge")
        metrics_content.append(f"jpeg_to_jxl_space_saved_bytes_total {self.total_space_saved_bytes}")

        # Space saved in the last (current) run
        metrics_content.append(
            f"# HELP jpeg_to_jxl_space_saved_bytes_last_interval Space saved in the last conversion interval in bytes.")
        metrics_content.append(f"# TYPE jpeg_to_jxl_space_saved_bytes_last_interval gauge")
        metrics_content.append(f"jpeg_to_jxl_space_saved_bytes_last_interval {self.last_interval_space_saved_bytes}")

        # Define the path for the metrics file
        metrics_file_name = "jxl_conversion_metrics.prom"
        metrics_file_path = os.path.join(self.metrics_directory, metrics_file_name)
        temp_metrics_file_path = metrics_file_path + ".tmp"  # Use a temporary file for atomic write

        try:
            # Write metrics to a temporary file first
            with open(temp_metrics_file_path, "w") as f:
                f.write("\n".join(metrics_content) + "\n")  # Add a newline at the end
            # Atomically replace the old metrics file with the new one
            os.replace(temp_metrics_file_path, metrics_file_path)
            logging.info(f"Metrics successfully written to {metrics_file_path}")
        except IOError as e:
            logging.error(f"Error writing metrics file {metrics_file_path}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while generating metrics file: {e}")

    def convert_image(self, input_filepath):
        """
        Converts a single JPEG/JPG file to JXL using the cjxl command-line tool.
        If successful, the original file is replaced by the new JXL file.
        If unsuccessful, the original file remains untouched.

        Args:
            input_filepath (str): The full path to the input JPEG/JPG file.

        Returns:
            tuple: (success (bool), original_size (int), converted_size (int),
                    duration (float), error_tag (str or None), full_error_message (str or None))
        """
        original_size = 0
        converted_size = 0
        duration = 0.0
        error_tag = None  # A standardized tag for Prometheus label
        full_error_message = None  # The detailed message for logging
        success = False

        # Determine the temporary output path for the JXL file
        # This ensures the original file is untouched until successful conversion
        temp_output_filepath = input_filepath + ".jxl.tmp"
        # Ensure the directory for the temporary JXL file exists
        os.makedirs(os.path.dirname(temp_output_filepath), exist_ok=True)

        try:
            original_size = os.path.getsize(input_filepath)
        except OSError as e:
            error_tag = "file_system_error"
            full_error_message = f"Failed to get original file size: {e}"
            logging.error(f"Error processing {input_filepath}: {full_error_message}")
            return False, 0, 0, 0, error_tag, full_error_message

        start_time = time.time()
        try:
            # Construct the cjxl command.
            # Default cjxl behavior is often near-lossless or lossless for JPEGs.
            # You can add options like -q (quality) or --lossless_jpeg if needed.
            command = [self.cjxl_path, input_filepath, temp_output_filepath]
            logging.debug(f"Executing cjxl: {' '.join(command)}")

            # Run the cjxl command
            # check=False allows us to handle non-zero exit codes manually
            result = subprocess.run(command, capture_output=True, text=True, check=False)
            end_time = time.time()
            duration = end_time - start_time

            if result.returncode == 0:
                # cjxl command executed successfully
                try:
                    converted_size = os.path.getsize(temp_output_filepath)
                    # Calculate final JXL filename (same base name, .jxl extension)
                    final_jxl_filepath = os.path.splitext(input_filepath)[0] + ".jxl"

                    # Atomically replace the original JPEG/JPG file with the JXL file
                    # First, remove the original file
                    os.remove(input_filepath)
                    # Then, rename the temporary JXL file to the final name
                    os.rename(temp_output_filepath, final_jxl_filepath)
                    success = True
                    logging.info(f"Successfully converted and replaced {input_filepath} -> {final_jxl_filepath}. "
                                 f"Original: {original_size} bytes, Converted: {converted_size} bytes.")
                except OSError as e:
                    error_tag = "file_system_error"
                    full_error_message = f"Post-conversion file operation failed (e.g., remove/rename): {e}. " \
                                         f"Original file might be missing or temp JXL not properly moved."
                    logging.error(f"Error during file replacement for {input_filepath}: {full_error_message}")
                    success = False  # Mark as failure due to incomplete operation
            else:
                # cjxl command failed (non-zero exit code)
                stderr_output = result.stderr.strip()
                full_error_message = f"cjxl failed with exit code {result.returncode}: {stderr_output}"

                # Determine a more specific error tag based on stderr content
                if "Error while decoding the JPEG image" in stderr_output:
                    error_tag = "corrupt_or_unsupported_jpeg"
                elif "unsupported input type" in stderr_output.lower():
                    error_tag = "unsupported_input_type"
                elif "out of memory" in stderr_output.lower():
                    error_tag = "cjxl_out_of_memory"
                elif "EncodeImageJXL() failed" in stderr_output:
                    error_tag = "cjxl_encoding_failed"
                else:
                    error_tag = "generic_cjxl_failure"  # Fallback if no specific phrase found

                logging.error(f"Conversion failed for {input_filepath}: {full_error_message}")
                success = False

        except FileNotFoundError:
            error_tag = "cjxl_not_found"
            full_error_message = f"cjxl command not found. Please ensure '{self.cjxl_path}' is in your PATH or provide the full path."
            logging.critical(full_error_message)  # Critical error, likely impacts all conversions
            success = False
        except subprocess.CalledProcessError as e:
            error_tag = "cjxl_execution_error_subprocess"  # For explicit CalledProcessError
            full_error_message = f"cjxl command execution error: {e.stderr.strip()}"
            logging.error(f"Conversion failed for {input_filepath}: {full_error_message}")
            success = False
        except Exception as e:
            error_tag = "unexpected_python_error"
            full_error_message = f"An unexpected error occurred during conversion process: {e}"
            logging.error(f"Unexpected error for {input_filepath}: {full_error_message}")
            success = False
        finally:
            # Clean up the temporary JXL file if it still exists (e.g., if post-conversion rename failed)
            if os.path.exists(temp_output_filepath):
                try:
                    os.remove(temp_output_filepath)
                    logging.debug(f"Cleaned up temporary file: {temp_output_filepath}")
                except OSError as e:
                    logging.warning(f"Could not remove temporary file {temp_output_filepath}: {e}")

        return success, original_size, converted_size, duration, error_tag, full_error_message

    def run_conversion(self):
        """
        Traverses the source directory, converts JPEG/JPG files, and updates metrics.
        After processing all files, it writes the aggregated metrics to a file.
        """
        logging.info(f"Starting JXL conversion process in '{self.source_directory}'...")
        self.last_interval_space_saved_bytes = 0  # Reset for the current run

        # Walk through the directory tree
        for root, _, files in os.walk(self.source_directory):
            for file in files:
                # Check for common JPEG file extensions (case-insensitive)
                if file.lower().endswith(('.jpg', '.jpeg')):
                    filepath = os.path.join(root, file)
                    logging.info(f"Processing image: {filepath}")

                    success, original_size, converted_size, duration, error_tag, full_error_message = self.convert_image(
                        filepath)

                    if success:
                        self.successful_conversions += 1
                        saved_bytes = original_size - converted_size
                        self.total_space_saved_bytes += saved_bytes
                        self.last_interval_space_saved_bytes += saved_bytes
                        logging.info(
                            f"  -> Successfully saved {saved_bytes} bytes (Original: {original_size}, JXL: {converted_size}).")
                    else:
                        self.failed_conversions += 1
                        # Use the error_tag directly as the reason_key for Prometheus label
                        reason_key = error_tag if error_tag else "unknown_error"  # Fallback if error_tag is None
                        self.failed_reasons[reason_key] += 1
                        logging.error(f"  -> Failed to convert {file}. Reason: {full_error_message}")

        # After processing all files, generate the metrics file
        self._generate_metrics_file()
        logging.info("JXL conversion process completed.")
        logging.info(f"--- Conversion Summary ---")
        logging.info(f"Total Images Processed: {self.successful_conversions + self.failed_conversions}")
        logging.info(f"Successful Conversions: {self.successful_conversions}")
        logging.info(f"Failed Conversions: {self.failed_conversions}")
        logging.info(f"Total Space Saved: {self.total_space_saved_bytes} bytes")
        logging.info(f"Space Saved This Run: {self.last_interval_space_saved_bytes} bytes")
        logging.info(f"Failure Reasons: {dict(self.failed_reasons)}")


# Main execution block to run the converter
if __name__ == "__main__":
    # Define default values for directories and cjxl path
    DEFAULT_SOURCE_DIR = "./images_to_convert"
    DEFAULT_METRICS_DIR = "/tmp/node_exporter_metrics"
    DEFAULT_CJXL_PATH = "cjxl"  # Assumes cjxl is in system's PATH

    # Get command-line arguments, or use defaults if not provided
    source_directory = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SOURCE_DIR
    metrics_directory = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_METRICS_DIR
    cjxl_command_path = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_CJXL_PATH

    # Initialize and run the converter
    converter_instance = None
    try:
        converter_instance = JxlConverter(source_directory, metrics_directory, cjxl_command_path)
        converter_instance.run_conversion()
    except FileNotFoundError as e:
        # Catch specific FileNotFoundError raised by JxlConverter if source_directory is problematic
        logging.critical(f"Initialization failed: {e}")
        logging.critical("Please ensure the source directory exists and is accessible.")
    except Exception as e:
        # Catch any other unexpected exceptions during initialization or runtime
        logging.critical(f"An unhandled critical error occurred: {e}", exc_info=True)
