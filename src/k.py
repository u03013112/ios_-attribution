from pynput.keyboard import Key, Controller
keyboard = Controller()
print('a')
# keyboard.press(Key.cmd.value)
keyboard.press('a')
keyboard.release('a')
# keyboard.release(Key.cmd.value)
print('a2')