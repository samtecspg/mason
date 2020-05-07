import connexion
import markdown
import operators as Operator
from util.printer import banner
from util.environment import MasonEnvironment
from configurations import get_all
from util.logger import logger

try:
    banner("Importing all registered_operator modules for API")
    env = MasonEnvironment()
    logger.set_level("trace")
    config = get_all(env)[0] # return first config by default
    Operator.import_all(env, config)
    swagger_yml = "api/base_swagger.yml"

    banner(f"Regenerating api yaml based on registered_operators to {swagger_yml}")
    Operator.update_yaml(env, swagger_yml)
    app = connexion.App(__name__, specification_dir='api')

    # Read the swagger.yml file to configure the endpoints
    app.add_api('swagger.yml')

    # Create a URL route in our application for "/"
    @app.route('/')
    def home():
        """
        This function just responds to the browser ULR
        localhost:5000/
        :return:        the rendered template 'home.html'
        """

        readme_file = open("README.md", "r")
        md_template_string = markdown.markdown(
            readme_file.read(), extensions=["fenced_code"]
        )

        return md_template_string

    # If we're running in stand alone mode, run the application
    if __name__ == '__main__':
        app.run(host='0.0.0.0', port=5000, debug=True)

except ModuleNotFoundError as e:
    logger.error("Mason not configured with registered_operators.  Please run 'mason config' and 'mason register' first.")
    logger.error(str(e))
