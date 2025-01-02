# src/utils/table_formatter.py
from typing import List, Any
from constants import *
class TableFormatter:
    """Handles all table formatting and display logic"""
    @staticmethod
    def get_printed_cell_size(cell: str) -> int:
        """Get size of cell when printed"""
        try:
            try:
                val = f"{int(cell):,}"
            except ValueError:
                val = f"{float(cell):,.2f}" 
        except ValueError:
            val = cell
        return len(val)

    @staticmethod
    def get_column_widths(headers: List[str], rows: List[List[Any]]) -> List[int]:
        """Calculate optimal width for each column"""
        widths = [len(str(header)) for header in headers]
        for row in rows:
            for i, cell in enumerate(row):
                widths[i] = max(widths[i], TableFormatter.get_printed_cell_size(str(cell)))
        return widths

    @staticmethod
    def print_horizontal_line(widths: List[int]) -> None:
        """Print a horizontal separator line"""
        line = "+"
        for width in widths:
            line += "-" * (width + 2) + "+"
        print(line)

    @staticmethod
    def print_row(row: List[Any], widths: List[int]) -> None:
        """Print a single row with proper spacing"""
        line = "|"
        for i, cell in enumerate(row):
            # print(cell)
            try:
                # val = f"{float(cell):.2f}" 
                try:
                    val = f"{int(cell):,}"
                except ValueError:
                    val = f"{float(cell):,.2f}" 
                # if cell can be converted to float get the string after round it to 0 decimal places
                # val = "{:.2f}".format(float(cell)) if isinstance(cell, float) else cell
                # if isinstance(cell, float):
                #     val = round(cell, 2) 
                # else:
                #     val = cell      
                # print(type(cell))                              
                # print(f"val={formatted}{type(cell)}{val}")
            except ValueError:
                val = cell
                # print(f"Error: {cell} is not a number")
                # exit(1)
            line += f" {val:<{widths[i]}} |"
        print(line)

    @staticmethod
    def print_table(headers: List[str], rows: List[List[Any]]) -> None:
        """Print complete table with headers and rows"""
        # Convert all values to strings and handle None
        str_rows = [[str(cell) if cell is not None else '' for cell in row] 
                   for row in rows]
        
        # Get column widths
        widths = TableFormatter.get_column_widths(headers, str_rows)
        
        # Print table
        TableFormatter.print_horizontal_line(widths)
        TableFormatter.print_row(headers, widths)
        TableFormatter.print_horizontal_line(widths)
        for row in str_rows:
            TableFormatter.print_row(row, widths)
        TableFormatter.print_horizontal_line(widths)
