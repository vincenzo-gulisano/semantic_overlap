import sys
import os
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.platypus import PageBreak
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib.units import cm

# Check if both input CSV and output PDF paths are provided as command-line arguments
if len(sys.argv) != 3:
    print("Usage: python script.py input.csv output.pdf")
    sys.exit(1)

# Get input CSV and output PDF file paths from command-line arguments
input_csv = sys.argv[1]
output_pdf = sys.argv[2]

# Read the CSV file into a DataFrame
df = pd.read_csv(input_csv)

# Create a PDF with one page for each line where outcome = 1
output = PdfWriter()

for _, row in df.iterrows():
    if row["outcome"] == 1:
        # Create a new PDF page
        c = canvas.Canvas("temp.pdf", pagesize=letter)
       
        # Define the position and width for text wrapping
        x, y = 100, 500
        max_width = 400
        
        # Extract the text from the row
        textToPrint = str("\n".join([f"{key}: {value}" for key, value in row.to_dict().items()]))
        # print(textToPrint)
                
        for i, line in enumerate(textToPrint.splitlines()):
            c.drawString(1 * cm, 29.7 * cm - 3 * cm - i * cm, line)

        # Show the page and add it to the output PDF
        c.showPage()
        c.save()
        with open("temp.pdf", "rb") as temp_pdf:
            output.add_page(PdfReader(temp_pdf).pages[0])


        # Check if 'folder' column contains a valid folder path
        folder = row["folder"]
        folder_path = os.path.join(folder, "summary_plots.pdf")
        if os.path.exists(folder_path):
            # Embed the 'summary_plots.pdf' into the current page
            pdf_to_append = PdfReader(open(folder_path, "rb"))
            # Loop through all pages and add them to the output PDF
            for page_num in range(len(pdf_to_append.pages)):
                pdf_page = pdf_to_append.pages[page_num]
                output.add_page(pdf_page)
        else:
            print(f"Warning: File not found in folder {folder}: 'summary_plots.pdf'")
        
# Remove any existing 'temp.pdf'
if os.path.exists("temp.pdf"):
    os.remove("temp.pdf")

# Save the output PDF
with open(output_pdf, "wb") as pdf_out:
    output.write(pdf_out)
