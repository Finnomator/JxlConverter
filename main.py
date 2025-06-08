import argparse
import hashlib  # For creating a stable identifier from directory paths
import logging
import os
import subprocess
import time
from collections import Counter

# Configure logging for better visibility into script execution
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class JxlConverter:
    """
    A class to convert JPEG/JPG files to JXL format and report metrics to Prometheus
    via the Node Exporter's textfile collector, with separate reports per directory.
    """

    def __init__(self, source_directories, metrics_root_directory, cjxl_path="cjxl"):
        """
        Initializes the JxlConverter for multiple source directories.

        Args:
            source_directories (list): A list of root directories to scan for JPEG/JPG files.
            metrics_root_directory (str): The root directory where Prometheus metric files will be written.
                                          Node Exporter should be configured to read from this path.
            cjxl_path (str): The path to the cjxl executable. Defaults to "cjxl"
                             assuming it's in the system's PATH.
        Raises:
            FileNotFoundError: If any of the specified source_directories do not exist and cannot be created.
        """
        self.cjxl_path = cjxl_path
        self.metrics_root_directory = metrics_root_directory
        self.metrics_data = {}  # Stores metrics for each source directory

        for s_dir in source_directories:
            abs_s_dir = os.path.abspath(s_dir)  # Use absolute path for consistency
            if not os.path.isdir(abs_s_dir):
                logging.warning(f"Source directory '{abs_s_dir}' not found.")
                try:
                    os.makedirs(abs_s_dir, exist_ok=True)
                    self._create_dummy_files(abs_s_dir)
                    logging.info(f"Source directory '{abs_s_dir}' created with dummy files.")
                except Exception as e:
                    raise FileNotFoundError(
                        f"Source directory not found and could not create it: {abs_s_dir}. Error: {e}")

            # Initialize metrics for each directory
            self.metrics_data[abs_s_dir] = {
                'total_conversions': 0,
                'successful_conversions': 0,
                'failed_conversions': 0,
                'failed_reasons': Counter(),
                'total_space_saved_bytes': 0,
                'last_interval_space_saved_bytes': 0,
                # New metrics for average size calculation
                'total_original_bytes_processed': 0,
                'total_converted_bytes_processed': 0
            }
            logging.info(f"Initialized metrics for directory: {abs_s_dir}")

        # Ensure the metrics root directory exists for Node Exporter
        os.makedirs(self.metrics_root_directory, exist_ok=True)
        logging.info(f"JXL Converter initialized.")
        logging.info(f"Scanning images in: {', '.join(self.metrics_data.keys())}")
        logging.info(f"Prometheus metrics will be written to: {self.metrics_root_directory}")
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
        Generates Prometheus-compatible metrics files for each processed directory
        in the specified metrics root directory.
        """
        for source_dir, metrics in self.metrics_data.items():
            metrics_content = []
            # Create a safe, unique identifier for the directory for the filename and labels
            # Using MD5 hash to ensure unique and safe filenames for metrics files
            dir_hash = hashlib.md5(source_dir.encode('utf-8')).hexdigest()
            # Sanitize directory path for Prometheus label (replace non-alphanumeric with underscore)
            # Using the original source_dir directly in the label is generally fine for Prometheus.
            prom_label_dir = source_dir.replace("\\", "/").replace(":", "_").replace(" ", "_")  # Simple sanitization

            # Total conversions attempted
            metrics_content.append(
                f"# HELP jpeg_to_jxl_conversions_total Total JPEG to JXL conversions attempted per directory.")
            metrics_content.append(f"# TYPE jpeg_to_jxl_conversions_total counter")
            metrics_content.append(
                f'jpeg_to_jxl_conversions_total{{directory="{prom_label_dir}"}} {metrics["total_conversions"]}')

            # Total successful conversions
            metrics_content.append(
                f"# HELP jpeg_to_jxl_conversions_successful_total Total successful JPEG to JXL conversions per directory.")
            metrics_content.append(f"# TYPE jpeg_to_jxl_conversions_successful_total counter")
            metrics_content.append(
                f'jpeg_to_jxl_conversions_successful_total{{directory="{prom_label_dir}"}} {metrics["successful_conversions"]}')

            # Total failed conversions, labeled by reason
            metrics_content.append(
                f"# HELP jpeg_to_jxl_conversions_failed_total Total failed JPEG to JXL conversions per directory.")
            metrics_content.append(f"# TYPE jpeg_to_jxl_conversions_failed_total counter")
            if not metrics['failed_reasons']:
                # Ensure the metric exists even if no failures occurred
                metrics_content.append(
                    f'jpeg_to_jxl_conversions_failed_total{{directory="{prom_label_dir}",reason="none"}} 0')
            else:
                for reason, count in metrics['failed_reasons'].items():
                    # Prometheus labels should be alphanumeric and underscores.
                    # Standardize common error reasons for cleaner labels.
                    standardized_reason = reason.replace(" ", "_").replace("-", "_").lower()
                    metrics_content.append(
                        f'jpeg_to_jxl_conversions_failed_total{{directory="{prom_label_dir}",reason="{standardized_reason}"}} {count}')

            # Cumulative total space saved
            metrics_content.append(
                f"# HELP jpeg_to_jxl_space_saved_bytes_total Total space saved by JXL conversions in bytes per directory.")
            metrics_content.append(f"# TYPE jpeg_to_jxl_space_saved_bytes_total gauge")
            metrics_content.append(
                f'jpeg_to_jxl_space_saved_bytes_total{{directory="{prom_label_dir}"}} {metrics["total_space_saved_bytes"]}')

            # Space saved in the last (current) run
            metrics_content.append(
                f"# HELP jpeg_to_jxl_space_saved_bytes_last_interval Space saved in the last conversion interval in bytes per directory.")
            metrics_content.append(f"# TYPE jpeg_to_jxl_space_saved_bytes_last_interval gauge")
            metrics_content.append(
                f'jpeg_to_jxl_space_saved_bytes_last_interval{{directory="{prom_label_dir}"}} {metrics["last_interval_space_saved_bytes"]}')

            # NEW: Cumulative total original bytes processed (for successful conversions)
            metrics_content.append(
                f"# HELP jpeg_to_jxl_original_bytes_processed_total Total bytes of original files processed successfully per directory.")
            metrics_content.append(f"# TYPE jpeg_to_jxl_original_bytes_processed_total gauge")
            metrics_content.append(
                f'jpeg_to_jxl_original_bytes_processed_total{{directory="{prom_label_dir}"}} {metrics["total_original_bytes_processed"]}')

            # NEW: Cumulative total converted bytes processed (for successful conversions)
            metrics_content.append(
                f"# HELP jpeg_to_jxl_converted_bytes_processed_total Total bytes of converted JXL files processed successfully per directory.")
            metrics_content.append(f"# TYPE jpeg_to_jxl_converted_bytes_processed_total gauge")
            metrics_content.append(
                f'jpeg_to_jxl_converted_bytes_processed_total{{directory="{prom_label_dir}"}} {metrics["total_converted_bytes_processed"]}')

            # Define the path for the metrics file specific to this directory
            metrics_file_name = f"jxl_conversion_metrics_{dir_hash}.prom"
            metrics_file_path = os.path.join(self.metrics_root_directory, metrics_file_name)
            temp_metrics_file_path = metrics_file_path + ".tmp"  # Use a temporary file for atomic write

            try:
                # Write metrics to a temporary file first
                with open(temp_metrics_file_path, "w") as f:
                    f.write("\n".join(metrics_content) + "\n")  # Add a newline at the end
                # Atomically replace the old metrics file with the new one
                os.replace(temp_metrics_file_path, metrics_file_path)
                logging.info(f"Metrics for '{source_dir}' successfully written to {metrics_file_path}")
            except IOError as e:
                logging.error(f"Error writing metrics file {metrics_file_path} for '{source_dir}': {e}")
            except Exception as e:
                logging.error(f"An unexpected error occurred while generating metrics file for '{source_dir}': {e}")

    def convert_image(self, input_filepath, metrics_for_current_dir):
        """
        Converts a single JPEG/JPG file to JXL using the cjxl command-line tool.
        If successful, the original file is replaced by the new JXL file.
        If unsuccessful, the original file remains untouched.

        Args:
            input_filepath (str): The full path to the input JPEG/JPG file.
            metrics_for_current_dir (dict): The dictionary holding metrics for the current source directory.

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
            command = [self.cjxl_path, input_filepath, temp_output_filepath]
            logging.debug(f"Executing cjxl: {' '.join(command)}")

            # Run the cjxl command
            result = subprocess.run(command, capture_output=True, text=True, check=False)
            end_time = time.time()
            duration = end_time - start_time

            if result.returncode == 0:
                # cjxl command executed successfully
                final_jxl_filepath = ""  # Define here to be available in except block
                try:
                    converted_size = os.path.getsize(temp_output_filepath)
                    final_jxl_filepath = os.path.splitext(input_filepath)[0] + ".jxl"

                    # First, rename the temporary JXL file to its final name.
                    os.rename(temp_output_filepath, final_jxl_filepath)

                    # --- MODIFICATION START ---
                    # Try to preserve timestamp. This is now a critical step for success.
                    try:
                        # Use 'touch -r' to copy timestamps. This is a critical step.
                        touch_command = ['touch', '-r', input_filepath, final_jxl_filepath]
                        subprocess.run(touch_command, check=True, capture_output=True, text=True)
                        logging.info(f"Successfully preserved timestamp for {final_jxl_filepath}.")

                        # Only if touch succeeds, remove original and mark as successful.
                        os.remove(input_filepath)
                        success = True
                        logging.info(f"Successfully converted and replaced {input_filepath} -> {final_jxl_filepath}. "
                                     f"Original: {original_size} bytes, Converted: {converted_size} bytes.")

                    except (FileNotFoundError, subprocess.CalledProcessError, Exception) as e:
                        # If touch fails, the entire operation is a failure.
                        success = False
                        error_tag = "timestamp_preservation_failed"

                        if isinstance(e, FileNotFoundError):
                            full_error_message = "'touch' command not found. Cannot preserve timestamp. Aborting replacement."
                        elif isinstance(e, subprocess.CalledProcessError):
                            full_error_message = f"Failed to preserve timestamp using 'touch' (Error: {e.stderr.strip()}). Aborting replacement."
                        else:
                            full_error_message = f"An unexpected error occurred during timestamp preservation ({e}). Aborting replacement."

                        logging.error(f"For {input_filepath}: {full_error_message}")

                        # IMPORTANT: Clean up the created JXL file and leave the original.
                        try:
                            os.remove(final_jxl_filepath)
                            logging.info(f"Removed incomplete JXL file: {final_jxl_filepath}")
                        except OSError as remove_error:
                            logging.error(f"Failed to remove incomplete JXL file {final_jxl_filepath}: {remove_error}")
                    # --- MODIFICATION END ---

                except OSError as e:
                    error_tag = "file_system_error"
                    full_error_message = f"Post-conversion file operation failed (e.g., rename/remove): {e}. " \
                                         f"Original file might be missing or temp JXL not properly moved."
                    logging.error(f"Error during file replacement for {input_filepath}: {full_error_message}")
                    success = False
            else:
                # cjxl command failed (non-zero exit code)
                stderr_output = result.stderr.strip()
                full_error_message = f"cjxl failed with exit code {result.returncode}: {stderr_output}"

                if "Error while decoding the JPEG image" in stderr_output:
                    error_tag = "corrupt_or_unsupported_jpeg"
                elif "unsupported input type" in stderr_output.lower():
                    error_tag = "unsupported_input_type"
                elif "out of memory" in stderr_output.lower():
                    error_tag = "cjxl_out_of_memory"
                elif "EncodeImageJXL() failed" in stderr_output:
                    error_tag = "cjxl_encoding_failed"
                else:
                    error_tag = "generic_cjxl_failure"

                logging.error(f"Conversion failed for {input_filepath}: {full_error_message}")
                success = False

        except FileNotFoundError:
            error_tag = "cjxl_not_found"
            full_error_message = f"cjxl command not found. Please ensure '{self.cjxl_path}' is in your PATH or provide the full path."
            logging.critical(full_error_message)
            success = False
        except subprocess.CalledProcessError as e:
            error_tag = "cjxl_execution_error_subprocess"
            full_error_message = f"cjxl command execution error: {e.stderr.strip()}"
            logging.error(f"Conversion failed for {input_filepath}: {full_error_message}")
            success = False
        except Exception as e:
            error_tag = "unexpected_python_error"
            full_error_message = f"An unexpected error occurred during conversion process: {e}"
            logging.error(f"Unexpected error for {input_filepath}: {full_error_message}")
            success = False
        finally:
            # Clean up the temporary JXL file if it still exists
            if os.path.exists(temp_output_filepath):
                try:
                    os.remove(temp_output_filepath)
                    logging.debug(f"Cleaned up temporary file: {temp_output_filepath}")
                except OSError as e:
                    logging.warning(f"Could not remove temporary file {temp_output_filepath}: {e}")

        return success, original_size, converted_size, duration, error_tag, full_error_message

    def run_conversion(self):
        """
        Traverses each configured source directory, converts JPEG/JPG files, and updates metrics
        for that specific directory. After processing all files, it writes the aggregated metrics
        to separate files for each directory.
        """
        logging.info("Starting JXL conversion process for all configured directories...")

        # Reset last interval space saved for all directories before starting
        for metrics in self.metrics_data.values():
            metrics['last_interval_space_saved_bytes'] = 0

        # Process each source directory
        for source_dir, metrics in self.metrics_data.items():
            logging.info(f"Processing images in directory: '{source_dir}'")
            # Walk through the directory tree for the current source_dir
            for root, _, files in os.walk(source_dir):
                for file in files:
                    # Check for common JPEG file extensions (case-insensitive)
                    if file.lower().endswith(('.jpg', '.jpeg')):
                        filepath = os.path.join(root, file)
                        logging.info(f"  Processing image: {filepath}")

                        # Pass the specific metrics dictionary for the current directory
                        success, original_size, converted_size, duration, error_tag, full_error_message = \
                            self.convert_image(filepath, metrics)

                        metrics['total_conversions'] += 1  # Increment total for this directory

                        if success:
                            metrics['successful_conversions'] += 1
                            saved_bytes = original_size - converted_size
                            metrics['total_space_saved_bytes'] += saved_bytes
                            metrics['last_interval_space_saved_bytes'] += saved_bytes
                            # Update new metrics for average size calculation
                            metrics['total_original_bytes_processed'] += original_size
                            metrics['total_converted_bytes_processed'] += converted_size

                            logging.info(
                                f"    -> Successfully saved {saved_bytes} bytes (Original: {original_size}, JXL: {converted_size}).")
                        else:
                            metrics['failed_conversions'] += 1
                            # Use the error_tag directly as the reason_key for Prometheus label
                            reason_key = error_tag if error_tag else "unknown_error"  # Fallback if error_tag is None
                            metrics['failed_reasons'][reason_key] += 1
                            logging.error(f"    -> Failed to convert {file}. Reason: {full_error_message}")
            logging.info(f"Finished processing directory: '{source_dir}'")

        # After processing all directories, generate the metrics files
        self._generate_metrics_file()
        logging.info("JXL conversion process completed for all directories.")
        logging.info(f"--- Global Conversion Summary ---")
        total_overall_successful = sum(m['successful_conversions'] for m in self.metrics_data.values())
        total_overall_failed = sum(m['failed_conversions'] for m in self.metrics_data.values())
        total_overall_saved = sum(m['total_space_saved_bytes'] for m in self.metrics_data.values())
        total_overall_processed = total_overall_successful + total_overall_failed

        logging.info(f"Overall Total Images Processed: {total_overall_processed}")
        logging.info(f"Overall Successful Conversions: {total_overall_successful}")
        logging.info(f"Overall Failed Conversions: {total_overall_failed}")
        logging.info(f"Overall Total Space Saved: {total_overall_saved} bytes")
        logging.info(f"\n--- Per-Directory Summaries ---")
        for source_dir, metrics in self.metrics_data.items():
            logging.info(f"Directory: '{source_dir}'")
            logging.info(f"  Total Processed: {metrics['total_conversions']}")
            logging.info(f"  Successful: {metrics['successful_conversions']}")
            logging.info(f"  Failed: {metrics['failed_conversions']}")
            logging.info(f"  Space Saved This Run: {metrics['last_interval_space_saved_bytes']} bytes")
            logging.info(f"  Total Space Saved for Dir: {metrics['total_space_saved_bytes']} bytes")
            logging.info(f"  Failure Reasons for Dir: {dict(metrics['failed_reasons'])}")
            logging.info("-" * 40)


# Main execution block to run the converter
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert JPEG/JPG images to JXL and report metrics for Prometheus Node Exporter.")
    parser.add_argument("source_directories", nargs='+',
                        help="One or more source directories to scan for JPEG/JPG files.")
    parser.add_argument("--metrics-dir", dest="metrics_directory",
                        default="/tmp/node_exporter_metrics",
                        help="The directory where Prometheus metric files will be written. Default: /tmp/node_exporter_metrics")
    parser.add_argument("--cjxl-path", dest="cjxl_command_path",
                        default="cjxl",
                        help="The path to the cjxl executable. Default: cjxl (assumes it's in PATH)")

    args = parser.parse_args()

    # Initialize and run the converter
    converter_instance = None
    try:
        # Use args.source_directories which is already a list
        converter_instance = JxlConverter(args.source_directories, args.metrics_directory, args.cjxl_command_path)
        converter_instance.run_conversion()
    except FileNotFoundError as e:
        # Catch specific FileNotFoundError raised by JxlConverter if source_directory is problematic
        logging.critical(f"Initialization failed: {e}")
        logging.critical("Please ensure the source directories exist and are accessible.")
    except Exception as e:
        # Catch any other unexpected exceptions during initialization or runtime
        logging.critical(f"An unhandled critical error occurred: {e}", exc_info=True)
