from appium import webdriver
from appium.options.android import UiAutomator2Options
from appium.webdriver.common.appiumby import AppiumBy

APPIUM_PORT = 4723
APPIUM_HOST = '127.0.0.1'

def create_android_driver(custom_opts = None):
    options = UiAutomator2Options()
    options.platformVersion = '11'
    options.udid = 'emulator-5554'
    if custom_opts is not None:
        options.load_capabilities(custom_opts)
    return webdriver.Remote(f'http://{APPIUM_HOST}:{APPIUM_PORT}', options=options)


def test_android_click(android_driver_factory):
    # Usage of the context manager ensures the driver session is closed properly
    # after the test completes. Otherwise, make sure to call `driver.quit()` on teardown.
    with android_driver_factory({
        # 'appium:app': 'com.android.settings',
        'appium:udid': 'emulator-5554',
    }) as driver:
        el = driver.find_element(by='xpath', value='//androidx.recyclerview.widget.RecyclerView[@resource-id="com.android.settings:id/recycler_view"]/android.widget.LinearLayout[4]')
        el.click()


# create_android_driver()

test_android_click(create_android_driver)