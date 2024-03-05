from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

from . import pokemon_list

class SourcePokemon(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        logger.info("Checking Pokemon API connection...")
        input_pokemon = config["pokemon_name"]
        if input_pokemon not in pokemon_list.POKEMON_LIST:
            result = f"Input Pokemon {input_pokemon} is invalid. Please check your spelling and input a valid Pokemon."
            logger.info(f"PokeAPI connection failed: {result}")
            return False, result
        else:
            logger.info(
                f"PokeAPI connection success: {input_pokemon} is a valid Pokemon"
            )
            return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [Pokemon(pokemon_name=config["pokemon_name"])]


class Pokemon(HttpStream):
    url_base = "https://pokeapi.co/api/v2/"

    # Set this as a noop.
    primary_key = None

    def __init__(self, pokemon_name: str, **kwargs):
        super().__init__(**kwargs)
        # Here's where we set the variable from our input to pass it down to the source.
        self.pokemon_name = pokemon_name

    def path(self, **kwargs) -> str:
        pokemon_name = self.pokemon_name
        # This defines the path to the endpoint that we want to hit.
        return f"pokemon/{pokemon_name}"

    def request_params(
        self,
        stream_state: Mapping[str, Any] | None,
        stream_slice: Mapping[str, Any] | None = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the Pokemon name as a query param so we do that in this method.
        return {"pokemon_name": self.pokemon_name}

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] | None  = None,
        next_page_token: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping]:
        # The response is a simple JSON whose schema matches our stream's schema exactly,
        # so we just return a list containing the response.
        return [response.json()]

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        # While the PokeAPI does offer pagination, we will only ever retrieve one Pokemon with this implementation,
        # so we just return None to indicate that there will never be any more pages in the response.
        return None
