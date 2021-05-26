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
    mainframe3 = Frame(root, bg='#3399ff')
    mainframe3.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))


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



    lbl_code = Label(mainframe3, bg='#3399ff', text="Additional Functions", font=('arial', 11), bd=10)
    lbl_code.place(x=570, y=0)

    add_btn = Button(mainframe3, text='Upload Whitelist', width=20)
    add_btn.place(x=480, y=50)
    add_btn.config(bg="LightBlue1")

    add_btn = Button(mainframe3, text='Download Whitelist', width=20)
    add_btn.place(x=480, y=90)
    add_btn.config(bg="LightBlue1")

    add_btn = Button(mainframe3, text='Download Scraper File', width=20)
    add_btn.place(x=480, y=130)
    add_btn.config(bg="LightBlue1")

    csvf = Button(mainframe3, text='Select file', width=38)
    csvf.place(x=665, y=50)




    run = Button(mainframe3, text="Run Scraper", width=20, height=2, bg="#009ACD")
    run.config(bg="LightBlue1")
    run.place(x=665, y=220)


    lbl_code = Label(mainframe3, bg='#3399ff', text="Compare to Fund Connect Fund Document Export", font=('arial', 11), bd=10)
    lbl_code.place(x=550, y=275)



    lbl_csvf = Label(mainframe3, bg='#3399ff', text="Input file", font=('arial', 12), bd=10)
    lbl_csvf.place(x=500, y=320)
    csvf = Button(mainframe3, text='Select file', width=38)
    csvf.place(x=665, y=325)


    lbl_date = Label(mainframe3, bg='#3399ff', text="Effective Date:", font=('arial', 12), bd=10)
    lbl_date.place(x=500, y=360)
    date = Entry(mainframe3, bg='white', textvariable=DATE, font=('arial', 12), width=30, fg='grey')
    date.place(x=665, y=365)
    date.insert(0, "MM/DD/YYYY")

    date.bind("<FocusIn>", handle_focus_in)
    date.bind("<FocusOut>", handle_focus_out)


    run = Button(mainframe3, text="Compare", width=20, height=2, bg="#009ACD")
    run.config(bg="LightBlue1")
    run.place(x=665, y=430)



    lbl_code = Label(mainframe3, bg='#3399ff', text="Click to next view after compare ran", font=('arial', 10), bd=10)
    lbl_code.place(x=600, y=660)


    image13 = Image.open("right.png")
    image13 = image13.resize((25, 25), Image.ANTIALIAS)
    photo13 = ImageTk.PhotoImage(image13)
    right_button = Button(mainframe3, image=photo13, border=0)
    right_button.config(bg="#3399ff")
    right_button.place(x=720, y=725)

    image23 = Image.open("left.png")
    image23 = image23.resize((25, 25), Image.ANTIALIAS)
    photo23 = ImageTk.PhotoImage(image23)
    left_button = Button(mainframe3, image=photo23, border=0)
    left_button.config(bg="#3399ff")
    left_button.place(x=680, y=725)

    root.mainloop()