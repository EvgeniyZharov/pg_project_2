import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure


class GraphWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Linear Graph Example")
        self.setGeometry(100, 100, 800, 600)

        self.figure = Figure()
        self.canvas = FigureCanvas(self.figure)
        self.ax = self.figure.add_subplot(111)

        x_values = [1, 2, 3, 4, 5]
        y_values = [2, 3, 5, 7, 11]

        self.ax.plot(x_values, y_values)

        layout = QVBoxLayout()
        layout.addWidget(self.canvas)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = GraphWindow()
    window.show()
    sys.exit(app.exec_())
