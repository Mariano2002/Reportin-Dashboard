# -*- coding: utf-8 -*-
import csv
import time
from datetime import date
import datetime
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tkinter import *
from threading import Thread
from tkinter import filedialog
import tkinter.messagebox as tkMessageBox
import shutil
from PIL import Image, ImageTk


if __name__ == '__main__':
    root = Tk()
    root.title("Fund Document Scraper \\ Menu")
    root.geometry("%dx%d+0+0" % (1400, 900))
    root.resizable(1, 1)
    root.config(bg="#3399ff")

    title = Frame(root, bd=1, relief=SOLID)
    title.pack(side=TOP, pady=10)
    lbl_display = Label(title, text="Fund Document Verification Tool", font=('arial', 20))
    lbl_display.pack(padx=240, pady=(5,35))
    mainframe1 = Frame(root, bg='#3399ff')
    mainframe1.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))



    NAME = StringVar()
    FNAME = StringVar()
    CODE = StringVar()
    DATE = StringVar()
    TYPEC = StringVar()
    PDF = IntVar()
    filename = None

    def handle_focus_in(_):
        date.delete(0, END)
        date.config(fg='black')

    def handle_focus_out(_):
        date.delete(0, END)
        date.config(fg='grey')
        date.insert(0, "Example: 12/31/2021")


    lbl_csvf = Label(mainframe1, bg='#3399ff', text="Input file", font=('arial', 12), bd=10)
    lbl_csvf.grid(row=0, pady=(75,0), sticky=W)
    csvf = Button(mainframe1, text='Select file', width=38)
    csvf.grid(row=0, column=1, pady=(75,0), sticky=W)

    lbl_date = Label(mainframe1, bg='#3399ff', text="Effective Date:", font=('arial', 12), bd=10)
    lbl_date.grid(row=1, sticky=W)
    date = Entry(mainframe1, bg='white', textvariable=DATE, font=('arial', 12), width=30, fg='grey')
    date.grid(row=1, column=1, sticky=W)
    date.insert(0, "MM/DD/YYYY")

    date.bind("<FocusIn>", handle_focus_in)
    date.bind("<FocusOut>", handle_focus_out)

    lbl_pdf = Label(mainframe1, bg='#3399ff', text="Scrape PDF's", font=('arial', 12), bd=10)
    lbl_pdf.grid(row=2, sticky=W)
    pdf = Checkbutton(mainframe1, variable=PDF, onvalue=1, offvalue=0)
    pdf.grid(row=2, column=1, sticky=W)
    pdf.config(bg="#3399ff")

    lbl_code = Label(mainframe1, bg='#3399ff', text="___________________________________________________________________________________________________________________________________", font=('arial', 12), bd=10)
    lbl_code.grid(row=3,columnspan=50, pady=(60,0), padx=(0,0))

    lbl_code = Label(mainframe1, bg='#3399ff', text="Manually add funds to Scraper", font=('arial', 12), bd=10)
    lbl_code.grid(row=4,columnspan=50, sticky=W)

    lbl_name = Label(mainframe1, bg='#3399ff', text="Provider Name:", font=('arial', 12), bd=10)
    lbl_name.grid(row=5, sticky=W)
    name = Entry(mainframe1, bg='white', textvariable=NAME, font=('arial', 12), width=30)
    name.grid(row=5, column=1, sticky=W)

    lbl_fname = Label(mainframe1, bg='#3399ff', text="Fund Name:", font=('arial', 12), bd=10)
    lbl_fname.grid(row=6, sticky=W)
    fname = Entry(mainframe1, bg='white', textvariable=FNAME, font=('arial', 12), width=30)
    fname.grid(row=6, column=1, sticky=W)

    lbl_typec = Label(mainframe1, bg='#3399ff', text="Fund Code Type:", font=('arial', 12), bd=10)
    lbl_typec.grid(row=7, sticky=W)
    code_types = ['ISIN', 'Cusip']
    typec = OptionMenu(mainframe1, TYPEC, *code_types)
    typec.config(width=38)
    typec.grid(row=7, column=1, sticky=W)
    TYPEC.set(code_types[0])

    lbl_code = Label(mainframe1, bg='#3399ff', text="Cusip/ISIN:", font=('arial', 12), bd=10)
    lbl_code.grid(row=8, sticky=W)
    code = Entry(mainframe1, bg='white', textvariable=CODE, font=('arial', 12), width=30)
    code.grid(row=8, column=1, sticky=W)


    add_btn = Button(mainframe1, text='Add Fund', width=10)
    add_btn.grid(row=9, column=1, sticky=W)
    add_btn.config(bg="LightBlue1")

    justone_btn = Button(mainframe1, text='Just this', width=20)
    #justone_btn.grid(row=10, column=1, columnspan=2)
    justone_btn.config(bg="LightBlue1")

    start_btn = Button(mainframe1, text='Start', width=20)
    #start_btn.grid(row=9, column=1, columnspan=2, pady=10)
    start_btn.config(bg="LightBlue1")

    download = Button(mainframe1, text="Download", width=20, bg="#009ACD")
    #download.grid(row=8, column=0, pady=10)
    download.config(bg="LightBlue1")
    download.config(state='disable')



    image = Image.open("right.png")
    image = image.resize((25, 25), Image.ANTIALIAS)
    photo = ImageTk.PhotoImage(image)
    right_button = Button(mainframe1, image=photo, border=0)
    right_button.config(bg="#3399ff")
    right_button.place(x=670, y=725)

    image2 = Image.open("left.png")
    image2 = image2.resize((25, 25), Image.ANTIALIAS)
    photo2 = ImageTk.PhotoImage(image2)
    left_button = Button(mainframe1, image=photo2, border=0)
    left_button.config(bg="#3399ff")
    left_button.place(x=630, y=725)



    root.mainloop()



