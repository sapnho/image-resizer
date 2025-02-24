import os
import time
import threading
import queue
from pathlib import Path
from PIL import Image
import pillow_heif
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Register HEIF opener with PIL
pillow_heif.register_heif_opener()

# Default configuration
PICTURES_FOLDER = "/home/pi/Pictures"
MAX_WIDTH = 1080
MAX_HEIGHT = 768
FILE_CHECK_RETRIES = 10
FILE_CHECK_INTERVAL = 0.1  # 100ms between checks
MAX_QUEUE_RETRIES = 5  # Maximum number of times to requeue a file

# Thread-safe queue to store new file paths
file_queue = queue.Queue()

def wait_for_file_stability(file_path, max_retries=FILE_CHECK_RETRIES, check_interval=FILE_CHECK_INTERVAL):
    """Wait for file to be completely written by checking if its size remains stable"""
    last_size = -1
    for _ in range(max_retries):
        try:
            current_size = os.path.getsize(file_path)
            if current_size == last_size and current_size > 0:
                return True
            last_size = current_size
            time.sleep(check_interval)
        except (OSError, FileNotFoundError):
            time.sleep(check_interval)
            continue
    return False

def resize_image(image_path, attempt=1):
    """ Resize image while maintaining aspect ratio and quality """
    try:
        print(f"Checking if file is ready: {image_path} (attempt {attempt})")
        if not wait_for_file_stability(image_path):
            if attempt < MAX_QUEUE_RETRIES:
                print(f"File not ready yet, requeueing: {image_path}")
                file_queue.put((image_path, attempt + 1))
            else:
                print(f"Max retries reached, skipping file: {image_path}")
            return
        
        print(f"Processing image: {image_path}")
        with Image.open(image_path) as img:
            # Get metadata if available, otherwise use None
            try:
                exif = img.info.get('exif', None)
            except:
                exif = None
            
            try:
                icc_profile = img.info.get('icc_profile', None)
            except:
                icc_profile = None
            
            # Convert to RGB if needed, preserving alpha channel if present
            if img.mode == 'RGBA':
                img = img
            elif img.mode != 'RGB':
                img = img.convert('RGB')

            # Check if resizing is needed
            width, height = img.size
            if width <= MAX_WIDTH and height <= MAX_HEIGHT:
                # If it's a HEIC file, we still need to convert it to JPEG
                if image_path.lower().endswith(('.heic', '.heif')):
                    new_path = os.path.splitext(image_path)[0] + '.jpg'
                    save_kwargs = {'format': 'JPEG', 'quality': 100, 'optimize': False}
                    if exif:
                        save_kwargs['exif'] = exif
                    if icc_profile:
                        save_kwargs['icc_profile'] = icc_profile
                    
                    img.save(new_path, **save_kwargs)
                    try:
                        os.remove(image_path)
                        print(f"Converted HEIC to JPEG: {new_path}")
                    except Exception as e:
                        print(f"Error removing original HEIC file: {str(e)}")
                else:
                    print(f"No resize needed for {image_path} ({width}x{height})")
                return

            print(f"Current image size: {width}x{height}")

            # Calculate memory requirements and use progressive loading if needed
            memory_estimate = (width * height * 3) / (1024 * 1024)  # In MB
            if memory_estimate > 500:  # If image might use more than 500MB
                print(f"Large image detected ({memory_estimate:.2f}MB), using progressive loading")
                img.draft('RGB', (width//2, height//2))

            # Resize while maintaining aspect ratio
            img.thumbnail((MAX_WIDTH, MAX_HEIGHT), Image.LANCZOS)
            new_width, new_height = img.size
            print(f"Resizing image from {width}x{height} to {new_width}x{new_height}")

            # Handle HEIC/HEIF files specially
            file_ext = image_path.lower().split('.')[-1]
            if file_ext in ['heic', 'heif']:
                new_path = os.path.splitext(image_path)[0] + '.jpg'
                save_kwargs = {
                    'format': 'JPEG',
                    'quality': 100,
                    'optimize': False
                }
                if exif:
                    save_kwargs['exif'] = exif
                if icc_profile:
                    save_kwargs['icc_profile'] = icc_profile
                
                img.save(new_path, **save_kwargs)
                try:
                    os.remove(image_path)
                    print(f"Converted and resized HEIC to JPEG: {new_path}")
                except Exception as e:
                    print(f"Error removing original HEIC file: {str(e)}")
            else:
                # For other formats, prepare save arguments
                save_kwargs = {}
                
                if file_ext.lower() in ['jpg', 'jpeg']:
                    save_kwargs['format'] = 'JPEG'
                    save_kwargs['quality'] = 100
                    save_kwargs['optimize'] = False
                    if exif:
                        save_kwargs['exif'] = exif
                elif file_ext.lower() == 'png':
                    save_kwargs['format'] = 'PNG'
                    save_kwargs['optimize'] = False
                elif file_ext.lower() == 'tiff':
                    save_kwargs['format'] = 'TIFF'
                    save_kwargs['compression'] = None
                
                if icc_profile:
                    save_kwargs['icc_profile'] = icc_profile
                
                img.save(image_path, **save_kwargs)
                print(f"Successfully saved resized image: {image_path}")

    except Exception as e:
        print(f"Error processing {image_path}: {str(e)}")
        if attempt < MAX_QUEUE_RETRIES:
            print(f"Requeueing due to error: {image_path}")
            file_queue.put((image_path, attempt + 1))

def is_image_file(filename):
    """Check if a file is an image based on its extension"""
    return filename.lower().endswith(('.png', '.jpg', '.jpeg', '.tiff', '.bmp', '.gif', '.heic', '.heif'))

def scan_existing_files():
    """Scan for existing images in the PICTURES_FOLDER and process them"""
    print(f"\nScanning existing files in {PICTURES_FOLDER}")
    count = 0
    for root, _, files in os.walk(PICTURES_FOLDER):
        for filename in files:
            if is_image_file(filename):
                file_path = os.path.join(root, filename)
                print(f"Found existing image: {file_path}")
                file_queue.put((file_path, 1))  # Start with attempt 1
                count += 1
    print(f"Initial scan completed. Found {count} images to process.\n")

def process_new_files():
    """ Processes new files from the queue """
    while True:
        try:
            item = file_queue.get()  # Get file from queue
            if isinstance(item, tuple):
                image_path, attempt = item
            else:
                image_path, attempt = item, 1  # Handle old-style queue items
            resize_image(image_path, attempt)
            file_queue.task_done()
        except Exception as e:
            print(f"Error in process_new_files: {str(e)}")

class ImageWatcher(FileSystemEventHandler):
    """ Watches for new image files and adds them to the queue """
    def on_created(self, event):
        if not event.is_directory and is_image_file(event.src_path):
            print(f"\nNew image detected: {event.src_path}")
            file_queue.put((event.src_path, 1))  # Start with attempt 1

def start_watching():
    """ Watches the PICTURES_FOLDER for new files """
    # First scan existing files
    scan_existing_files()
    
    # Then start watching for new files
    observer = Observer()
    event_handler = ImageWatcher()
    observer.schedule(event_handler, PICTURES_FOLDER, recursive=True)
    observer.start()
    print(f"Watching for new images in {PICTURES_FOLDER}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping image watcher...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    # Start the worker thread that processes images in the queue
    worker_thread = threading.Thread(target=process_new_files, daemon=True)
    worker_thread.start()

    start_watching()
