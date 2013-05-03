Packaging the ReFlow Uploader GUI for Windows and Mac OS X
====

This document provides basic instructions for creating both a Windows XP/Vista/7
executable and a Mac OS X application bundle. The resulting packages will not
require the user to pre-install Python or any other software. These are
stand-alone applications with the Python interpreter and all necessary
dependencies included within the application.

Dependencies
====

The ReFlow Uploader GUI uses the ReFlowRESTClient Python module, and the GUI
source code is included in that project within the 'bin' directory. The
following Python dependencies need to be installed within the development
and packaging environment:

* requests >= 1.1.0
* PIL >= 1.1.7

The other Python modules used, including Tkinter and ttk, are part of the Python
standard library.

In addition, the PyInstaller package is needed, but should not be installed via
'easy_install' or 'pip'. Make sure to get version > 2.0. As of this writing,
the official 2.1 version has not been released, so you will have to get the
developmental versionDownload the PyInstaller here:

http://www.pyinstaller.org/

Do not use version 2.0, it will not work due to some issues with the PIL module
as well another issue with OS X 10.8 (Mountain Lion).

PyInstaller requires pywin32, available here:

﻿http://sourceforge.net/projects/pywin32

Finally, PyInstaller requires Python version 2.4 through 2.7. Python 3 is not
supported. Both the Windows and Mac packaging procedures were tested on Python
version 2.7.4.

Creating a Windows Executable - Windows 7
====

#.  Download the Python 2.7.4 installer for Windows:

    ﻿http://www.python.org/download/releases/2.7.4/

#.  Install Python 2.7.4 using the downloaded MSI file.

#.  Download pywin32 build 218 for Python 2.7:

    ﻿http://sourceforge.net/projects/pywin32/files/pywin32/Build%20218/pywin32-218.win32-py2.7.exe/download

#.  Install pywin32.

#.  Download the PyInstaller zip file (remember to get version > 2.0):

    ﻿http://www.pyinstaller.org/

#.  Unzip the PyInstall zip file to the desktop (or wherever you want)

#.  Download and run pip-Win so you can install pip (and virtualenv).

    ﻿https://sites.google.com/site/pydatalog/python/pip-for-windows

#.  After pip-Win installs it will display a simple GUI to run pip commands.
    Use this GUI or the command prompt to install requests:

    ``pip install requests``

#.  You cannot install PIL via pip, so download the Windows installer here:

    ﻿http://effbot.org/downloads/PIL-1.1.7.win32-py2.7.exe

#.  Install PIL from the downloaded executable.

#.  Download the ReFlowRESTClient code from Github:

    ﻿https://github.com/whitews/ReFlowRESTClient/archive/master.zip

#.  Unzip the ReFlowRESTClient zip file to the desktop (or wherever you want)

#.  Open a Windows command prompt and change to the PyInstaller directory

    ``cd Desktop\pyinstaller-pyinstaller-ccb6f3d``

#.  Run the pyinstaller.py script with the following options:

    ``pyinstaller.py -w --onefile --hidden-import=reflowrestclient -p ..\ReFlowRESTClient-master\ ..\ReFlowRESTClient-master\bin\ReFlowUploader.py``

#.  PyInstaller should have created a new ReFlowUploader directory within the
    pyinstaller directory. Within that directory should be a build and dist
    directory along with a spec file. The resulting exe file in the dist
    directory will not work, since the image and icon paths are not
    specified. We need to edit the spec file in a text editor to add the path
    to the image resources. Add the following lines in the spec file:

    ``﻿a.datas += [
                     ('reflow_text.png','C:\\Users\\swhite\\Desktop\\ReFlowRESTClient-master\\resources\\reflow_text.png','DATA'),
                     ('reflow.ico','C:\\Users\\swhite\\Desktop\\ReFlowRESTClient-master\\resources\\reflow.ico','DATA')
                 ]``

    Note: Change the paths above to reflect the actual location of the files on
    your system.

#.  From the command prompt, run PyInstaller on the spec file:

    `pyinstaller.py --onefile ReFlowUploader\ReFlowUploader.spec`

#.  Test that the resulting exe file in the ReFlowUploader\dist folder runs
    correctly.

Creating a Mac Application Bundle - Mac OS X (10.8.3)
====

