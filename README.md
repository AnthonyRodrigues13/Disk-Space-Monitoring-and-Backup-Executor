# Disk-Space-Monitoring-and-Backup-Executor
This repository features a comprehensive solution for monitoring disk space and managing backup operations:

C Code: Implements a real-time disk space monitoring tool using fanotify that listens for and logs file operations such as copying and deleting. This tool helps maintain an accurate view of file activity and usage on the system, crucial for detecting anomalies and managing disk resources efficiently.

Python Code: A flexible command executor and log manager that handles backup operations, executes commands, and updates a PostgreSQL database with detailed logs of each operation. It automates tasks related to file backups, ensuring that data integrity is maintained and important events are recorded for audit and recovery purposes.

This repository is ideal for server administrators and developers looking to streamline disk management, automate backup tasks, and maintain a secure log of all file-related operations.
