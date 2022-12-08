import pygame
pygame.init()
pygame.joystick.init()

joystick_count = pygame.joystick.get_count()
print(joystick_count)
stick = pygame.joystick.Joystick(0)
stick.init()

clock = pygame.time.Clock()
done = False
while not done:
    try:
        for event in pygame.event.get():
            print(event)
    except KeyboardInterrupt:
        done = True
pygame.quit()